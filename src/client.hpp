#ifndef GFS_CLIENT_HPP
#define GFS_CLIENT_HPP

#include "common.hpp"
#include <user.hpp>

class Client
{
public:
	uint64_t dataIDAlloc;
	Client(LightDS::User &srv)
	{
		this->srv = srv;
		dataIDAlloc = 0;	
	}
	

public:
	// FINISHED
	// Create creates a new file on the specific path on GFS.
	GFSError 
		Create(const std::string &path)
		{
			GFSError mid = srv.RPCCall(srv.ListService("master")[0], "RPCCreateFile", path);	
			return mid;
		}

	// FINISHED
	// Mkdir creates a new directory on GFS.
	GFSError 
		Mkdir(const std::string &path)
		{
			GFSError mid = srv.RPCCall(srv.ListService("master")[0], "RPCMkdir", path);
			return mid;	
		}

	// FINISHED
	// List lists files and directories in specific directory on GFS.
	std::tuple<GFSError, std::vector<std::string> /*filenames*/>
		List(const std::string &path)
		{
			return srv.RPCCall(srv.ListService("master")[0], "RPCListFile", path);	
		}
	
	// FINISHED
	// Read reads the file at specific offset.
	// It reads up to data.size() bytes form the File.
	// It return the number of bytes, and an error if any.
	std::tuple<GFSError, size_t /*byteOfRead*/>
		Read(const std::string &path, std::uint64_t offset, std::vector<char> &data)
		{
			uint64_t readSize = data.size();
			std::vector<std::tuple<ChunkHandle, uint64_t, uint64_t>> readList;
			uint64_t off = offset;
			while(true)
			{
				std::tuple<GFSError, ChunkHandle> mid = srv.RPCCaller(srv.ListService("master")[0], "RPCGetChunkHandle", path, off / Chunk::chunkLength);	
				if(mid.errCode != GFSErrorCode::OK)
					return mid;
				if(off % Chunk::chunkLength + readSize <= Chunk::chunkLength)
				{
					readList.push_back(make_tuple(mid.get<1>, off % Chunk::chunkLength, readSize));
					break;
				}
				readList.push_back(make_tuple(mid.get<1>, off % Chunk::chunkLength, Chunk::chunkLength - off % Chunk::chunklength);
				readSize -= (Chunk::chunkLength - off % Chunk::chunkLength);
				off = 0;
			}
			std::string ans = "";
			for(int i = 0; i < readList.size(); i++)
			{
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetPrimaryAndSecondaries", readList[i].get<0>);
				std::tuple<GFSError, std::string> ansTuple = srv.RPCCall(mid.get<1>, "RPCReadChunk", readList[i].get<0>, readList[i].get<1>, readList[i].get<2>);
				ans += ansTuple.get<1>;
			}
			int len = ans.length();
			for(int i = 0; i < len; i++)
				data[i] = ans[i];
			return make_tuple(GFSError(GFSErrorCode::OK), len);
		}

	// FINISHED
	// Write writes data to the file at specific offset.
	GFSError 
		Write(const std::string &path, std::uint64_t offset, const std::vector<char> &data)
		{
			uint64_t writeSize = data.size();
			int vctNow = 0;
			std::vector<std::tuple<ChunkHandle, uint64_t, std::string>> writeList;
			uint64_t off = offset;
			while(true)
			{
				std::tuple<GFSError, ChunkHandle> mid = srv.RPCCaller(srv.ListService("master")[0], "RPCGetChunkHandle", path, off / Chunk::chunkLength);	
				if(mid.errCode != GFSErrorCode::OK)
					return mid;
				if(off % Chunk::chunkLength + writeSize <= Chunk::chunkLength)
				{
					std::string chuanru;
					chuanru.clear();
					for(int j = vctNow; j < vctNow + writeSize; j++)
						chuanru += data[j];
					vctNow += writeSize;
					writeList.push_back(make_tuple(mid.get<1>, off % Chunk::chunkLength, chuanru));
					break;
				}
				std::string chuanru;
				chuanru.clear();
				for(int j = vctNow; j < vctNow + Chunk::chunkLength - off % Chunk::chunklength; j++)
					chuanru += data[j];
				vctNow += Chunk::chunkLength - off % Chunk::chunklength;
				writeList.push_back(make_tuple(mid.get<1>, off % Chunk::chunkLength(), chuanru);
				writeSize -= (Chunk::chunkLength - off % Chunk::chunkLength);
				off = 0;
			}
			for(int i = 0; i < writeList.size(); i++)
			{
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetPrimaryAndSecondaries", readList[i].get<0>);
				uint64_t curID = dataIDAlloc++;
				GFSError mid_pri = srv.RPCCall(mid.get<1>, "RPCPushData", curID, writeList.get<2>);
				for(int j = 0; j < secondaries.size(); j++)
				{
					GFSError mid_sec = srv.RPCCall(mid.get<2>[j], "RPCPushData", curID, writeList.get<2>);	
				}
				std::tuple<GFSError, std::string> ansTuple = srv.RPCCall(mid.get<1>, "RPCWriteChunk", writeList[i].get<0>, curID, writeList[i].get<1>, readList[i].get<2>);
			}
			return GFSError(GFSErrorCode::OK);
		}

	// FINISHED
	// Append appends data to the file. Offset of the beginning of appended data is returned.
	std::tuple<GFSError, std::uint64_t /*offset*/>
		Append(const std::string &path, const std::vector<char> &data)
		{
			uint64_t appendSize = data.size();
			int num = srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandleForAppend", path);
			std::tuple<GFSError, ChunkHandle> mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandle", path, num);
			std::string dataString;
			for(int i = 0; i < data.size(); i++)
				dataString += data[i];
			std::tuple<GFSError, std::string, std::vector<std::string>> pas = srv.RPCCall(srv.ListService("master")[0], "RPCGetPrimaryAndSecondaries", mid.get<1>);
			uint64_t curID = dataIDAlloc++;
			GFSError mid_pri = srv.RPCCall(pas.get<1>, "RPCPushData", curID, dataString);
			uint64_t off = srv.RPCCall(pas.get<1>, "getFileSizeFromHandle", mid.get<1>);
			for(int i = 0; i < secondaries.size(); i++)
				GFSError mid_sec = srv.RPCCall(pas.get<2>[i], "RPCPushData", curID, dataString);
			std::tuple<GFSError, std::uint64_t> ans = srv.RPCCall(pas.get<1>, "RPCAppendChunk", mid.get<1>, curID, pas.get<2>);
			if(ans.get<0>.errCode == GFSErrorCode::retryAppend)
			{
				mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandle", path, num + 1);
				pas = srv.RPCCall(srv.ListService("master")[0], "RPCGetPrimaryAndSecondaries", mid.get<1>);
				ans = srv.RPCCall(pas.get<1>, "RPCAppendChunk", mid.get<1>, curID, pas.get<2>);
				off = 0;
			}
			return make_tuple(GFSError(GFSError::OK), off);	
			
		}

public:
	// FINISHED
	// GetChunkHandle returns the chunk handle of (path, index).
	// If the chunk doesn't exist, create one.
	std::tuple<GFSError, ChunkHandle>
		GetChunkHandle(const std::string &path, std::uint64_t index)
		{
			return srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandle", path, index);
		}

	// FINISHED
	// ReadChunk reads data from the chunk at specific offset.
	// data.size()+offset  should be within chunk size.
	std::tuple<GFSError, size_t /*byteOfRead*/>
		ReadChunk(ChunkHandle handle, std::uint64_t offset, std::vector<char> &data)
		{
			uint64_t len = data.size();
			std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t> pas = srv.RPCCall(srv.ListService("master")[0], "GetPrimaryAndSecondaries", handle);
			std::tuple<GFSError, std::string>mid = srv.RPCCall(pas.get<1>, "RPCReadChunk", handle, offset, len);
			for(int i = 0; i < mid.get<1>.size(); i++)
				data[i] = mid.get<1>[i];
			return make_tuple(GFSError(GFSErrorCode::OK), mid.get<1>.size());
		}
	
	// FINISHED
	// WriteChunk writes data to the chunk at specific offset.
	// data.size()+offset should be within chunk size.
	GFSError
		WriteChunk(ChunkHandle handle, std::uint64_t offset, const std::vector<char> &data)
		{
			std::string dataString;
			for(int i = 0; i < data.size(); i++)
				dataString += data[i];
			std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t> pas = srv.RPCCall(srv.ListService("master")[0], "GetPrimaryAndSecondaries", handle);
			uint64_t curID = dataIDAlloc++;
			GFSError mid_pri = srv.RPCCall(pas.get<1>, "RPCPushData", curID, dataString);
			for(int i = 0; i < pas.get<2>.size(); i++)
			{
				GFSError mid_sec = srv.RPCCall(pas.get<2>[i], "RPCPushData", curID, dataString);	
			}
			GFSError ans = srv.RPCCall(pas.get<1>, "RPCWriteChunk", handle, curID, offset, pas.get<2>);
			return ans;
		}
	
	// FINISHED
	// AppendChunk appends data to a chunk.
	// Chunk offset of the start of data will be returned if success.
	// data.size() should be within max append size.
	std::tuple<GFSError, std::uint64_t /*offset*/>
		AppendChunk(ChunkHandle handle, const std::vector<char> &data)
		{
			std::string dataString;
			for(int i = 0; i < data.size(); i++)
				dataString += data[i];
			std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t> pas = srv.RPCCall(srv.ListService("master")[0], "GetPrimaryAndSecondaries", handle);
			uint64_t curID = dataIDAlloc++;
			GFSError mid_pri = srv.RPCCall(pas.get<1>, "RPCPushData", curID, dataString);
			for(int i = 0; i < pas.get<2>.size(); i++)
			{
				GFSError mid_sec = srv.RPCCall(pas.get<2>[i], "RPCPushData", curID, dataString);	
			}
			uint64_t off = srv.RPCCall(pas.get<1>, "getFileSizeFromHandle", handle);
			std::tuple<GFSError, std::uint64_t> ans = srv.RPCCall(pas.get<1>, "RPCAppendChunk", mid.get<1>, curID, pas.get<2>);
			return ans;
		}

protected:
	LightDS::User &srv;
};

#endif
