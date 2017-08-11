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
	uint64_t serialNo;
	bool isPrimary;
	time_t expireTimeStamp;
	
public:
	static const uint64_t chunkLength = 67108864;	// = 64MB
	std::mutex readLock; 
	std::mutex writeLock;
	
	Chunk()
	{
		handle = version = 0;
		isPrimary = 0;
		expireTimeStamp = 0;
		serialNo = 0;
	}
	
	
};

#endif
