#ifndef GFS_CHUNK_HPP
#define GFS_CHUNK_HPP

#include <string>
#include <mutex>

class Chunk
{
	friend class Master;
	friend class ChunkServer;
private:
	ChunkHandle handle;
	ChunkVersion version;
	static uint64_t chunkLength = 67108864;	// = 64MB
	uint64_t serialNo;
	bool isPrimary;
	time_t expireTimeStamp;
	
public:
	std::mutex readLock; 
	std::mutex writeLock;
	
	Chunk(ChunkHandle hd, ChunkVersion vs = 0)
	{
		handle = hd;
		version = vs;
		isPrimary = 0;
		expireTimeStamp = 0;
		serialNo = 0;
	}
	
	
};

#endif
