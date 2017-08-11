#ifndef GFS_CLIENT_HPP
#define GFS_CLIENT_HPP

#include "common.hpp"
#include <user.hpp>

class Client
{
public:
	uint64_t dataIDAlloc;
	Client(LightDS::User &srv) : srv(srv)
	{
		static uint64_t dataIDAlloc = 0;	
	}
	

public:

	void dataIDAllocPlusOne()
	{
		dataIDAlloc++;
	}

	// FINISHED
	// Create creates a new file on the specific path on GFS.
	GFSError 
		Create(const std::string &path)
		{
			GFSError mid = srv.RPCCall(srv.ListService("master")[0], "RPCCreateFile", path).get().as<GFSError>();	
			std::cerr << "OUT\n";
			return mid;
		}

	// FINISHED
	// Mkdir creates a new directory on GFS.
	GFSError 
		Mkdir(const std::string &path)
		{
			GFSError mid = srv.RPCCall(srv.ListService("master")[0], "RPCMkdir", path).get().as<GFSError>();
			return mid;	
		}

	// FINISHED
	// List lists files and directories in specific directory on GFS.
	std::tuple<GFSError, std::vector<std::string> /*filenames*/>
		List(const std::string &path)
		{
			return srv.RPCCall(srv.ListService("master")[0], "RPCListFile", path).get().as<std::tuple<GFSError, std::vector<std::string>>>();	
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
				std::tuple<GFSError, ChunkHandle> mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandle", path, off / Chunk::chunkLength).get().as<std::tuple<GFSError, ChunkHandle>>();	
				if(std::get<0>(mid).errCode != GFSErrorCode::OK)
					return mid;
				if(off % Chunk::chunkLength + readSize <= Chunk::chunkLength)
				{
					readList.push_back(std::make_tuple(std::get<1>(mid), off % Chunk::chunkLength, readSize));
					break;
				}
				readList.push_back(std::make_tuple(std::get<1>(mid), off % Chunk::chunkLength, Chunk::chunkLength - off % Chunk::chunkLength));
				readSize -= (Chunk::chunkLength - off % Chunk::chunkLength);
				off = 0;
			}
			std::string ans = "";
			for(int i = 0; i < readList.size(); i++)
			{
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetPrimaryAndSecondaries", std::get<0>(readList[i])).get().as<std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t>>();
				std::tuple<GFSError, std::string> ansTuple = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(mid)), "RPCReadChunk", std::get<0>(readList[i]), std::get<1>(readList[i]), std::get<2>(readList[i])).get().as<std::tuple<GFSError, std::string>>();
				ans += std::get<1>(ansTuple);
			}
			int len = ans.length();
			for(int i = 0; i < len; i++)
				data[i] = ans[i];
			return std::make_tuple(GFSError(GFSErrorCode::OK), len);
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
				std::tuple<GFSError, ChunkHandle> mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandle", path, off / Chunk::chunkLength).get().as<std::tuple<GFSError, ChunkHandle>>();	
				if(std::get<0>(mid).errCode != GFSErrorCode::OK)
					return std::get<0>(mid);
				if(off % Chunk::chunkLength + writeSize <= Chunk::chunkLength)
				{
					std::string chuanru;
					chuanru.clear();
					for(int j = vctNow; j < vctNow + writeSize; j++)
						chuanru += data[j];
					vctNow += writeSize;
					writeList.push_back(std::make_tuple(std::get<1>(mid), off % Chunk::chunkLength, chuanru));
					break;
				}
				std::string chuanru;
				chuanru.clear();
				for(int j = vctNow; j < vctNow + Chunk::chunkLength - off % Chunk::chunkLength; j++)
					chuanru += data[j];
				vctNow += Chunk::chunkLength - off % Chunk::chunkLength;
				writeList.push_back(std::make_tuple(std::get<1>(mid), off % Chunk::chunkLength, chuanru));
				writeSize -= (Chunk::chunkLength - off % Chunk::chunkLength);
				off = 0;
			}
			for(int i = 0; i < writeList.size(); i++)
			{
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetPrimaryAndSecondaries", std::get<0>(writeList[i])).get().as<std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t>>();
				uint64_t curID = dataIDAlloc;
				dataIDAllocPlusOne();
				GFSError mid_pri = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(mid)), "RPCPushData", curID, std::get<2>(writeList[i])).get().as<GFSError>();
				for(int j = 0; j < std::get<2>(mid).size(); j++)
				{
					GFSError mid_sec = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<2>(mid)[j]), "RPCPushData", curID, std::get<2>(writeList[i])).get().as<GFSError>();	
				}
				std::tuple<GFSError, std::string> ansTuple = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(mid)), "RPCWriteChunk", std::get<0>(writeList[i]), curID, std::get<1>(writeList[i]), std::get<2>(writeList[i])).get().as<std::tuple<GFSError, std::string>>();
			}
			return GFSError(GFSErrorCode::OK);
		}

	// FINISHED
	// Append appends data to the file. Offset of the beginning of appended data is returned.
	std::tuple<GFSError, std::uint64_t /*offset*/>
		Append(const std::string &path, const std::vector<char> &data)
		{
			uint64_t appendSize = data.size();
			int num = srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandleForAppend", path).get().as<int>();
			std::tuple<GFSError, ChunkHandle> mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandle", path, num).get().as<std::tuple<GFSError, ChunkHandle>>();
			std::string dataString;
			for(int i = 0; i < data.size(); i++)
				dataString += data[i];
			std::tuple<GFSError, std::string, std::vector<std::string>> pas = srv.RPCCall(srv.ListService("master")[0], "RPCGetPrimaryAndSecondaries", std::get<1>(mid)).get().as<std::tuple<GFSError, std::string, std::vector<std::string>>>();
			uint64_t curID = dataIDAlloc;
			dataIDAllocPlusOne();
			GFSError mid_pri = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "RPCPushData", curID, dataString).get().as<GFSError>();
			uint64_t off = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "getFileSizeFromHandle", std::get<1>(mid)).get().as<uint64_t>();
			for(int i = 0; i < std::get<2>(pas).size(); i++)
				GFSError mid_sec = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<2>(pas)[i]), "RPCPushData", curID, dataString).get().as<GFSError>();
			std::tuple<GFSError, std::uint64_t> ans = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "RPCAppendChunk", std::get<1>(mid), curID, std::get<2>(pas)).get().as<std::tuple<GFSError, std::uint64_t>>();
			if(std::get<0>(ans).errCode == GFSErrorCode::retryAppend)
			{
				mid = srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandle", path, num + 1).get().as<std::tuple<GFSError, ChunkHandle>>();
				pas = srv.RPCCall(srv.ListService("master")[0], "RPCGetPrimaryAndSecondaries", std::get<1>(mid)).get().as<std::tuple<GFSError, std::string, std::vector<std::string>>>();
				ans = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "RPCAppendChunk", std::get<1>(mid), curID, std::get<2>(pas)).get().as<std::tuple<GFSError, std::uint64_t>>();
				off = 0;
			}
			return std::make_tuple(GFSError(GFSErrorCode::OK), off);	
			
		}

public:
	// FINISHED
	// GetChunkHandle returns the chunk handle of (path, index).
	// If the chunk doesn't exist, create one.
	std::tuple<GFSError, ChunkHandle>
		GetChunkHandle(const std::string &path, std::uint64_t index)
		{
			return srv.RPCCall(srv.ListService("master")[0], "RPCGetChunkHandle", path, index).get().as<std::tuple<GFSError, ChunkHandle>>();
		}

	// FINISHED
	// ReadChunk reads data from the chunk at specific offset.
	// data.size()+offset  should be within chunk size.
	std::tuple<GFSError, size_t /*byteOfRead*/>
		ReadChunk(ChunkHandle handle, std::uint64_t offset, std::vector<char> &data)
		{
			uint64_t len = data.size();
			std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t> pas = srv.RPCCall(srv.ListService("master")[0], "GetPrimaryAndSecondaries", handle).get().as<std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t>>();
			std::tuple<GFSError, std::string> mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "RPCReadChunk", handle, offset, len).get().as<std::tuple<GFSError, std::string>>();
			for(int i = 0; i < std::get<1>(mid).size(); i++)
				data[i] = std::get<1>(mid)[i];
			return std::make_tuple(GFSError(GFSErrorCode::OK), std::get<1>(mid).size());
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
			std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t> pas = srv.RPCCall(srv.ListService("master")[0], "GetPrimaryAndSecondaries", handle).get().as<std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t>>();
			uint64_t curID = dataIDAlloc;
			dataIDAllocPlusOne();
			GFSError mid_pri = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "RPCPushData", curID, dataString).get().as<GFSError>();
			for(int i = 0; i < std::get<2>(pas).size(); i++)
			{
				GFSError mid_sec = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<2>(pas)[i]), "RPCPushData", curID, dataString).get().as<GFSError>();	
			}
			GFSError ans = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "RPCWriteChunk", handle, curID, offset, std::get<2>(pas)).get().as<GFSError>();
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
			std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t> pas = srv.RPCCall(srv.ListService("master")[0], "GetPrimaryAndSecondaries", handle).get().as<std::tuple<GFSError, std::string, std::vector<std::string>, uint64_t>>();
			uint64_t curID = dataIDAlloc;
			dataIDAllocPlusOne();
			GFSError mid_pri = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "RPCPushData", curID, dataString).get().as<GFSError>();
			for(int i = 0; i < std::get<2>(pas).size(); i++)
			{
				GFSError mid_sec = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<2>(pas)[i]), "RPCPushData", curID, dataString).get().as<GFSError>();	
			}
			uint64_t off = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "getFileSizeFromHandle", handle).get().as<uint64_t>();
			std::tuple<GFSError, std::uint64_t> ans = srv.RPCCall(LightDS::User::RPCAddress::from_string(std::get<1>(pas)), "RPCAppendChunk", handle, curID, std::get<2>(pas)).get().as<std::tuple<GFSError, std::uint64_t>>();
			return ans;
		}

protected:
	LightDS::User &srv;
};

#endif
