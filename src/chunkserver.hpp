#ifndef GFS_CHUNKSERVER_HPP
#define GFS_CHUNKSERVER_HPP

#include "common.hpp"
#include "chunk.hpp"
#include <string>
#include <service.hpp>
#include <vector>
#include <fstream>
#include <map>
#include <sstream>
#include <ctime>

class ChunkServer
{
public:
	enum MutationType : std::uint32_t
	{
		MutationWrite,
		MutationAppend,
		MutationPad
	};
	
	std::string chtos(ChunkHandle a)
	{
		ostringstream os;
		os << a;	
		std::string result;
		istingstream is(os.str());
		is >> result;
		reutrn result;
	}
	
	uint64_t getFileSize(const char* strFileName)
	{
	    FILE * fp = fopen(strFileName, "r");
	    fseek(fp, 0L, SEEK_END);
	    uint64_t size = ftell(fp);
	    fclose(fp);
	    return size;
	}
	
	uint64_t getFileSizeFromHandle(ChunkHandle handle)
	{
		return getFileSize((serverID + "_" + chtos(handle) + ".chunk").c_str());
	}
	
	void bind(std::string rpcName, Ret(*func)(Args...))
	{
		srv.RPCBind(rpcName, std::function<Ret(Args...)>([this, func](Args ...args) -> Ret 
		{
			return (this->*func)(std::forward<Args>(args)...);
		}));
	}

	ChunkServer(LightDS::Service &srv, const std::string &rootDir)
	{
		this->srv = srv;
		this->rootDir = rootDir;
		running = 0;
		serverID = srv.getLocalRPCPort();	
		
		bind("RPCAppendChunk", &RPCAppendChunk);
		bind("RPCApplyCopy", &RPCApplyCopy);
		bind("RPCApplyMutation", &RPCApplyMutation);
		bind("RPCCreateChunk", &RPCCreateChunk);
		bind("RPCGrantLease", &RPCGrantLease);
		bind("RPCPushData", &RPCPushData);
		bind("RPCReadChunk", &RPCReadChunk);
		bind("RPCSendCopy", &RPCSendCopy);
		bind("RPCUpdateVersion", &RPCUpdateVersion);
		bind("RPCWriteChunk", &RPCWriteChunk);
		bind("RPCDeleteChunk", &RPCDeleteChunk);
		bind("getFileSizeFromHandle", &getFileSizeFromHandle);
	}
	void Start()
	{
		running = 1;
		
		chunkList.clear();
		chunkMap.clear();
		dataMap.clear();
	}
	void Shutdown()
	{
		running = 0;
	}
	std::string serverID;

protected:
	LightDS::Service &srv;
	std::string rootDir;
	static time_t heartbeatTime = 30;
	bool running;

public:
	std::map<ChunkHandle, Chunk*> chunkMap;
	std::map<std::u64int_t, std::string> dataMap;
	
	std::vector<ChunkHandle> leaseExtensions;
	std::vector<ChunkHandle> failedChunks;
	
	//FINISHED
	void BackgroundActivities()
	{
		while(true)
		{
			if(time(NULL) % heartbeatTime == 0)
			{
				Heartbeat();	
			}
			if(!running)
			{
				break;	
			}
		}
	}
	
	//FINISHED
	void Heartbeat()
	{
		std::tuple<GFSError, std::vector<ChunkHandle>> mid;
		std::map<ChunkHandle, Chunk*>::iterator it;
		std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks;
		for(it = chunkMap.begin(); it != chunkMap.end(); it++)
		{
			ChunkHandle hd = (*it)->handle;
			ChunkVersion vs = (*it)->version;
			std::tuple<ChunkHandle, ChunkVersion> tpl = make_tuple(hd, vs);
			chunks.push_back(tpl);
		}
		mid = srv.RPCCall(srv.ListService("master")[0], "RPCHeartbeat", leaseExtensions, chunks, failedChunks);
		if(mid.get<0>.errCode != GFSErrorCode::OK)
			break;
		std::vector<std::string> garbageChunks = mid.get<1>;
		for(int i = 0; i < garbageChunks.size(); i++)
		{
			Chunk* curC = chunkMap[garbageChunks[i]];
			delete curC;
			std::remove(char* (serverID + "_" + chtos(garbageChunks) + ".chunks").c_str());
		}
		leaseExtensions.clear();
		failChunks.clear();
	}

	// FINISHED -- added by myself
	// delete chunks for another chunk (which is dead)
	GFSError
		RPCDeleteChunk(std::string serverID, ChunkHandle handle)
		{
			std::remove(char* (serverID + "_" + chtos(handle) + ".chunk").c_str());	
			if(srv.getLocalRPCPort() == serverID)
			{
				delete chunkMap[handle];
				chunkMap.erase(handle); 
			}
			return GFSError(GFSErrorCode::OK);
		}

	// FINISHED
	// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
	GFSError
		RPCCreateChunk(ChunkHandle handle)
		{
			Chunk* now = new Chunk(handle);
			chunkMap[handle] = now;
			GFSError Csq(GFSErrorCode::OK)
			return Csq;
		}

	// FINISHED
	// RPCReadChunk is called by client, read chunk data and return
	std::tuple<GFSError, std::string /*Data*/>
		RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length)
		{
			if(!chunkMap.count(handle))
			{
				GFSError Csq(GFSErrorCode::invalidHandle);
				std::tuple<GFSError, std::string> Rtn = make_tuple(Csq, "");
				return Rtn;	
			}
			ifstream infile;
			infile.open(serverID + "_" + chtos(handle) + ".chunk");
			infile.seekg(offset);
			std::string ans;
			infile.read(ans, length);
			infile.close();
			GFSError Csq(GFSErrorCode::OK);
			std::tuple<GFSError, std::string> Rtn = make_tuple(Csq, ans);
			return Rtn;
		}

	// FINISHED
	// RPCWriteChunk is called by client
	// applies chunk write to itself (primary) and asks secondaries to do the same.
	GFSError
		RPCWriteChunk(ChunkHandle handle, std::uint64_t dataID, std::uint64_t offset, std::vector<std::string> secondaries)
		{
			if(!chunkMap.count(handle))
			{
				GFSError Csq(GFSErrorCode::invalidHandle);
				return Csq;
			}
			leaseExtensions.push_back(handle);
			ofstream outfile;
			outfile.open(serverID + "_" + handle + ".chunk");
			outfile.seekp(offset);
			outfile << dataMap[dataID];
			outfile.close();
			Chunk* curC = chunkMap[handle];
			curC->serialNo++;
			for(int i = 0; i < secondaries.size(); i++)
			{
				GFSError mid = srv.RPCCall(secondaries[i], "RPCApplyMutation", handle, curC->serialNo, MutationWrite, dataID, offset, dataMap[dataID].length());	
				if(mid.errCode != GFSErrorCode::OK)
				{
					return mid;	
				}
			}
			GFSError Csq(GFSErrorCode::OK);
			return Csq;
		}
		
	// FINISHED
	// RPCAppendChunk is called by client to apply atomic record append.
	// The length of data should be within max append size.
	// If the chunk size after appending the data will excceed the limit,
	// pad current chunk and ask the client to retry on the next chunk.
	std::tuple<GFSError, std::uint64_t /*offset*/>
		RPCAppendChunk(ChunkHandle handle, std::uint64_t dataID, std::vector<std::string> secondaries)
		{
			std::string data = dataMap[dataID];
			uint64_t fileSize = getFileSize((serverID + "_" + chtos(handle) + ".chunk").c_str());
			if(fileSize + (uint64_t)data.length() > Chunk::chunkLength)
			{	
				ofstream outfile;
				outfile.open(serverID + "_" + chtos(handle) + ".chunk");
				outfile.seekp(fileSize);
				for(int i = 0; i < Chunk::chunkLength - fileSize; i++)
					outfile.write('*');
				chunkMap[handle]->serialNo++;
				for(int i = 0; i < secondaries.size(); i++)
				{
					GFSError mid = srv.RPCCall(secondaries[i], "RPCApplyMutation", handle, chunkMap[handle]->serialNo, MutationPad, dataID, fileSize, data.length());
					if(mid.errCode != GFSErrorCode::OK)
						return make_tuple(mid, 0);
				}
				GFSError Csq(GFSErrorCode::retryAppend);
				return make_tuple(Csq, 0);
			}
			ofstream outfile;
			outfile.open(serverID + "_" + chtos(handle) + ".chunk");
			outfile.seekp(fileSize);
			outfile.write(data);
			chunkMap[handle]->serialNo++;
			for(int i = 0; i < secondaries.size(); i++)
			{
				GFSError mid = srv.RPCCall(secondaries[i], "RPCApplyMutation", handle, chunkMap[handle]->serialNo, MutationAppend, dataID, fileSize, data.length());
				if(mid.errCode != GFSErrorCode::OK)
					return make_tuple(mid, 0);
			}
			GFSError Csq(GFSErrorCode::OK);
			leaseExtensions.push_back(handle);
			return make_tuple(Csq, fileSize);
		}

	// FINISHED
	// RPCApplyMutation is called by primary to apply mutations
	GFSError
		RPCApplyMutation(ChunkHandle handle, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset, std::uint64_t length);
		{
			if(!chunkMap.count(handle))
			{
				GFSError Csq(GFSErrorCode::invalidHandle);
				return Csq;
			}
			if(type == MutationWrite)
			{
				Chunk* curC = chunkMap[handle];
				if(curC->serialNo != serialNo - 1)
				{
					GFSError Csq(GFSErrorCode::wrongSerialNo);
					return Csq;
				}
				ofstream outfile;
				outfile.open(serverID + "_" + handle + ".chunk");
				outfile.seekp(offset);
				outfile << dataMap[dataID];
				outfile.close();
				curC->serialNo++;
				GFSError Csq(GFSErrorCode::OK);
				return Csq;
			}
			else if(type == MutaionAppend)
			{
				Chunk* curC = chunkMap[handle];
				if(curC->serialNo != serialNo - 1)
				{
					GFSError Csq(GFSErrorCode::wrongSerialNo);
					return Csq;
				}
				ofstream outfile;
				outfile.open(serverID + "_" + chtos(handle) + ".chunk");
				outfile.seekp(fileSize);
				outfile.write(data);
				curC->serialNo++;
				GFSError Csq(GFSErrorCode::OK);
				return Csq;
			}
			else
			{
				Chunk* curC = chunkMap[handle];
				if(curC->serialNo != serialNo - 1)
				{
					GFSError Csq(GFSErrorCode::wrongSerialNo);
					return Csq;
				}
				ofstream outfile;
				outfile.open(serverID + "_" + chtos(handle) + ".chunk");
				outfile.seekp(fileSize);
				for(int i = 0; i < Chunk::chunkLength - fileSize; i++)
					outfile.write('*');
				curC->serialNo++;
				GFSError Csq(GFSErrorCode::OK);
				return Csq;
			}
		}
		
	// FINISHED
	// RPCSendCopy is called by master, send the whole copy to given address
	GFSError
		RPCSendCopy(ChunkHandle handle, std::string addr)
		{
			if(!chunkMap.count(handle))
			{
				GFSError Csq(GFSErrorCode::invalidHandle);
				return Csq;	
			}
			Chunk* curC = chunkMap[handle];
			ifstream infile;
			infile.open(serverID + "_" + chtos(handle) + ".chunk");
			std::string data;
			infile.read(data, Chunk::chunkLength);
			infile.close();
			GFSError mid = srv.RPCCall(addr, "RPCApplyCopy", handle, curC->version, data, curC->serialNo);
			return mid;
		}

	// FINISHED
	// RPCApplyCopy is called by another replica
	// rewrite the local version to given copy data
	GFSError
		RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo)
		{
			if(!chunkMap[handle])
			{
				Chunk* curC = new Chunk;
				chunkMap[handle] = curC;
				ofstream outfile;
				outfile.open(serverID + "_" + chtos(handle) + ".chunk", ofstream::trunc);
				outfile << data;
				outfile.close();
				curC->version = version;
				curC->serialNo = serialNo;
			}
			else
			{
				Chunk* curC = chunkMap[handle];
				ofstream outfile;
				outfile.open(serverID + "_" + chtos(handle) + ".chunk", ofstream::trunc);
				outfile << data;
				outfile.close();
				curC->version = version;
				curC->serialNo = serialNo;
			}
		}

	// FINISHED
	// RPCGrantLease is called by master
	// mark the chunkserver as primary
	GFSError
		RPCGrantLease(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>> vct)
		{
			bool flag = 1;
			for(int i = 0; i < vct.size(); i++)
			{
				ChunkHandle hd = vct[i].get<0>;
				ChunkVersion vs = vct[i].get<1>;
				uint64_t expireTS = vct[i].get<2>;
				if(!chunkMap.count(hd))
				{
					flag = 0;
					continue;
				}
				Chunk* curC = chunkMap[hd];
				curC->handle = hd;
				curC->version = vs;
				curC->isPrimary = 1;
				curC->expireTimeStamp = expireTS;
			}
			if(flag)
			{
				GFSError Csq(GFSErrorCode::OK);
				return Csq;	
			}
			else
			{
				GFSError Csq(GFSErrorCode::invalidHandle);
				return Csq;	
			}
		}

	// FINISHED
	// RPCUpdateVersion is called by master
	// update the given chunks' version to 'newVersion'
	GFSError
		RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion)
		{
			if(!chunkMap.count(handle))
			{
				GFSError Csq;
				Csq.errCode = GFSErrorCode::invalidHandle;
				return Csq;	
			}
			chunkMap[handle]->version = newVersion;
			GFSError Csq;
			Csq.errCode = GFSErrorCode::OK;
			return Csq;
		}

	// FINISHED
	// RPCPushDataAndForward is called by client.
	// It saves client pushed data to memory buffer.
	// This should be replaced by a chain forwarding.
	GFSError
		RPCPushData(std::uint64_t dataID, std::string data)
		{
			if(dataMap.count(dataID))
			{
				GFSError Csq;
				Csq.errCode = GFSErrorCode::dataIDUsed;
				return Csq;	
			}
			dataMap[dataID] = data;
			GFSError Csq;
			Csq.errCode = GFSErrorCode::OK;
			return Csq;
		}
};

MSGPACK_ADD_ENUM(ChunkServer::MutationType);

#endif
