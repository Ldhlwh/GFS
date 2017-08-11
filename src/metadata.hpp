#ifndef GFS_METADATA_HPP
#define GFS_METADATA_HPP

#include "master.hpp"
#include "chunkserver.hpp"
#include <ctime>
#include <deque>

class FileMeta
{
	friend class Master;
private:
	ChunkHandle handle;
	bool deleted;
	time_t delTimeStamp;
	
public:
	FileMeta()
	{
		handle = delTimeStamp = 0;
		deleted = 0;	
	}
	FileMeta(ChunkHandle a)
	{
		handle = a;
		delTimeStamp = 0;
		deleted = 0;
	}
};

class ChunkMeta
{
	friend class Master;	
private:
	ChunkHandle handle, preHandle, nexHandle;
	std::deque<std::string> atServer;
	std::string mainChunk;
	time_t expireTimeStamp;
	long long version;//to check whether the replica of a chunk is the latest version
	
public:
	ChunkMeta()
	{
		preHandle = nexHandle = 0;
		mainChunk = "";
		expireTimeStamp = version = 0;
	}
};

class ServerMeta
{
	friend class Master;
private:
	std::deque<ChunkHandle> handleList;
	time_t lastHeartBeat;
	
public:
	ServerMeta()
	{
		lastHeartBeat = 0;
	}
};

#endif 
