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
		std::ostringstream os;
		os << a;	
		std::string result;
		std::istringstream is(os.str());
		is >> result;
		return result;
	}
	
	std::string prtos(std::uint16_t a)
	{
		std::ostringstream os;
		os << a;
		std::string result;
		std::istringstream is;
		is >> result;
		return result;
	}
	
	uint64_t getFileSize(std::string strFileName)
	{
	    FILE * fp = fopen(strFileName.c_str(), "r");
	    fseek(fp, 0L, SEEK_END);
	    uint64_t size = ftell(fp);
	    fclose(fp);
	    return size;
	}
	
	uint64_t getFileSizeFromHandle(ChunkHandle handle)
	{
		return getFileSize((prtos(serverPort) + "_" + chtos(handle) + ".chunk"));
	}
	
	ChunkServer(LightDS::Service &srv, const std::string &rootDir) : srv(srv), rootDir(rootDir)
	{
		running = 0;
		serverPort = srv.getLocalRPCPort();	
		
		srv.RPCBind<GFSError(std::uint16_t, ChunkHandle)>("RPCDeleteChunk", std::bind(&ChunkServer::RPCDeleteChunk, this, std::placeholders::_1, std::placeholders::_2));
		srv.RPCBind<GFSError(ChunkHandle)>("RPCCreateChunk", std::bind(&ChunkServer::RPCCreateChunk, this, std::placeholders::_1));
		srv.RPCBind<std::tuple<GFSError, std::string>(ChunkHandle, std::uint64_t, std::uint64_t)>("RPCReadChunk", std::bind(&ChunkServer::RPCReadChunk, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
		srv.RPCBind<GFSError(ChunkHandle, std::uint64_t, std::uint64_t, std::vector<std::string>)>("RPCWriteChunk", std::bind(&ChunkServer::RPCWriteChunk, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));
		srv.RPCBind<std::tuple<GFSError, std::uint64_t>(ChunkHandle, std::uint64_t, std::vector<std::string>)>("RPCAppendChunk", std::bind(&ChunkServer::RPCAppendChunk, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
		srv.RPCBind<GFSError(ChunkHandle, std::uint64_t, MutationType, std::uint64_t, std::uint64_t, std::uint64_t)>("RPCApplyMutation", std::bind(&ChunkServer::RPCApplyMutation, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5, std::placeholders::_6));
		srv.RPCBind<GFSError(ChunkHandle, std::string)>("RPCSendCopy", std::bind(&ChunkServer::RPCSendCopy, this, std::placeholders::_1, std::placeholders::_2));
		srv.RPCBind<GFSError(ChunkHandle, ChunkVersion, std::string, std::uint64_t)>("RPCApplyCopy", std::bind(&ChunkServer::RPCApplyCopy, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));
		srv.RPCBind<GFSError(std::vector<std::tuple<ChunkHandle, ChunkVersion, std::uint64_t>>)>("RPCGrantLease", std::bind(&ChunkServer::RPCGrantLease, this, std::placeholders::_1));
		srv.RPCBind<GFSError(ChunkHandle, ChunkVersion)>("RPCUpdateVersion", std::bind(&ChunkServer::RPCUpdateVersion, this, std::placeholders::_1, std::placeholders::_2));
		srv.RPCBind<GFSError(std::uint64_t, std::string)>("RPCPushData", std::bind(&ChunkServer::RPCPushData, this, std::placeholders::_1, std::placeholders::_2));
		srv.RPCBind<uint64_t(std::string)>("getFileSize", std::bind(&ChunkServer::getFileSize, this, std::placeholders::_1));
		srv.RPCBind<uint64_t(ChunkHandle)>("getFileSizeFromHandle", std::bind(&ChunkServer::getFileSizeFromHandle, this, std::placeholders::_1));
	
	}
	void Start()
	{
		std::cerr << "ChunkServer : " << srv.getLocalRPCPort() << " start" << std::endl;
		running = 1;
		chunkMap.clear();
		dataMap.clear();
		BackgroundActivities();
	}
	void Shutdown()
	{
		running = 0;
	}
	std::uint16_t serverPort;

protected:
	LightDS::Service &srv;
	std::string rootDir;
	static const time_t heartbeatTime =  30;
	bool running;

public:
	std::map<ChunkHandle, Chunk*> chunkMap;
	std::map<std::uint64_t, std::string> dataMap;
	
	std::vector<ChunkHandle> leaseExtensions;
	std::vector<ChunkHandle> failedChunks;
	
	//FINISHED
	void BackgroundActivities()
	{
		Heartbeat();
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
			ChunkHandle hd = it->first;
			ChunkVersion vs = it->second->version;
			std::tuple<ChunkHandle, ChunkVersion> tpl = std::make_tuple(hd, vs);
			chunks.push_back(tpl);
		}
		mid = srv.RPCCall(srv.ListService("master")[0], "RPCHeartbeat", leaseExtensions, chunks, failedChunks).get().as<std::tuple<GFSError, std::vector<ChunkHandle>>>();
		if(std::get<0>(mid).errCode != GFSErrorCode::OK)
			return;
		std::vector<ChunkHandle> garbageChunks = std::get<1>(mid);
		for(int i = 0; i < garbageChunks.size(); i++)
		{
			Chunk* curC = chunkMap[garbageChunks[i]];
			delete curC;
			std::remove((prtos(serverPort) + "_" + chtos(garbageChunks[i]) + ".chunks").c_str());
		}
		leaseExtensions.clear();
		failedChunks.clear();
	}

	// FINISHED -- added by myself
	// delete chunks for another chunk (which is dead)
	GFSError
		RPCDeleteChunk(std::uint16_t serverPort, ChunkHandle handle)
		{
			std::remove((prtos(serverPort) + "_" + chtos(handle) + ".chunk").c_str());	
			if(srv.getLocalRPCPort() == serverPort)
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
			Chunk* now = new Chunk;
			now->handle = handle;
			chunkMap[handle] = now;
			GFSError Csq(GFSErrorCode::OK);
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
				std::tuple<GFSError, std::string> Rtn = std::make_tuple(Csq, "");
				return Rtn;	
			}
			std::ifstream infile;
			infile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk");
			infile.seekg(offset);
			char input[Chunk::chunkLength + 5];
			infile.read(input, length);
			std::string ans = input;
			infile.close();
			GFSError Csq(GFSErrorCode::OK);
			std::tuple<GFSError, std::string> Rtn = std::make_tuple(Csq, ans);
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
			std::ofstream outfile;
			outfile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk");
			outfile.seekp(offset);
			std::string output = dataMap[dataID];
			outfile.write(output.c_str(), output.length());
			outfile.close();
			Chunk* curC = chunkMap[handle];
			curC->serialNo++;
			for(int i = 0; i < secondaries.size(); i++)
			{
				GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(secondaries[i]), "RPCApplyMutation", handle, curC->serialNo, MutationWrite, dataID, offset, (uint64_t)dataMap[dataID].length()).get().as<GFSError>();	
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
			uint64_t fileSize = getFileSize((prtos(serverPort) + "_" + chtos(handle) + ".chunk"));
			if(fileSize + (uint64_t)data.length() > Chunk::chunkLength)
			{	
				std::ofstream outfile;
				outfile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk");
				outfile.seekp(fileSize);
				for(int i = 0; i < Chunk::chunkLength - fileSize; i++)
					outfile.write("*", 1);
				chunkMap[handle]->serialNo++;
				for(int i = 0; i < secondaries.size(); i++)
				{
					GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(secondaries[i]), "RPCApplyMutation", handle, chunkMap[handle]->serialNo, MutationPad, dataID, fileSize, (uint64_t)data.length()).get().as<GFSError>();
					if(mid.errCode != GFSErrorCode::OK)
						return std::make_tuple(mid, 0);
				}
				GFSError Csq(GFSErrorCode::retryAppend);
				return std::make_tuple(Csq, 0);
			}
			std::ofstream outfile;
			outfile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk");
			outfile.seekp(fileSize);
			outfile.write(data.c_str(), data.length());
			chunkMap[handle]->serialNo++;
			for(int i = 0; i < secondaries.size(); i++)
			{
				GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(secondaries[i]), "RPCApplyMutation", handle, chunkMap[handle]->serialNo, MutationAppend, dataID, fileSize, (uint64_t)data.length()).get().as<GFSError>();
				if(mid.errCode != GFSErrorCode::OK)
					return std::make_tuple(mid, 0);
			}
			GFSError Csq(GFSErrorCode::OK);
			leaseExtensions.push_back(handle);
			return std::make_tuple(Csq, fileSize);
		}

	// FINISHED
	// RPCApplyMutation is called by primary to apply mutations
	GFSError
		RPCApplyMutation(ChunkHandle handle, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset, std::uint64_t length)
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
				std::ofstream outfile;
				outfile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk");
				outfile.seekp(offset);
				std::string output = dataMap[dataID];
				outfile.write(output.c_str(), output.length());
				outfile.close();
				curC->serialNo++;
				GFSError Csq(GFSErrorCode::OK);
				return Csq;
			}
			else if(type == MutationAppend)
			{
				Chunk* curC = chunkMap[handle];
				if(curC->serialNo != serialNo - 1)
				{
					GFSError Csq(GFSErrorCode::wrongSerialNo);
					return Csq;
				}
				std::ofstream outfile;
				outfile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk");
				outfile.seekp(offset);
				std::string input = dataMap[dataID];
				outfile.write(input.c_str(), input.length());
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
				std::ofstream outfile;
				outfile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk");
				outfile.seekp(offset);
				for(int i = 0; i < Chunk::chunkLength - offset; i++)
					outfile.write("*", 1);
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
			std::ifstream infile;
			infile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk");
			char input[Chunk::chunkLength + 5];
			infile.read(input, Chunk::chunkLength);
			std::string data = input;
			infile.close();
			GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(addr), "RPCApplyCopy", handle, curC->version, data, curC->serialNo).get().as<GFSError>();
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
				std::ofstream outfile;
				outfile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk", std::ofstream::trunc);
				outfile.write(data.c_str(), data.length());
				outfile.close();
				curC->handle = handle;
				curC->version = version;
				curC->serialNo = serialNo;
			}
			else
			{
				Chunk* curC = chunkMap[handle];
				std::ofstream outfile;
				outfile.open(prtos(serverPort) + "_" + chtos(handle) + ".chunk", std::ofstream::trunc);
				outfile.write(data.c_str(), data.length());
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
				ChunkHandle hd = std::get<0>(vct[i]);
				ChunkVersion vs = std::get<1>(vct[i]);
				uint64_t expireTS = std::get<2>(vct[i]);
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
