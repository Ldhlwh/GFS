#ifndef GFS_MASTER_H
#define GFS_MASTER_H

#include "common.h"
#include <string>
#include <service.hpp>

class Master
{
public:
	Master(LightDS::Service &srv, const std::string &rootDir);
	void Start();
	void Shutdown();

protected:
	// BackgroundActivity does all the background activities:
	// dead chunkserver handling, garbage collection, stale replica detection, etc
	void BackgroundActivity();

	// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
	std::tuple<GFSError, std::vector<ChunkHandle> /*Garbage Chunks*/>
		RPCHeartbeat(std::vector<ChunkHandle> leaseExtensions, std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks, std::vector<ChunkHandle> failedChunks);

	// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
	// If no one holds the lease currently, grant one.
	std::tuple<GFSError, std::string /*Primary Address*/, std::vector<std::string> /*Secondary Addresses*/, std::uint64_t /*Expire Timestamp*/>
		RPCGetPrimaryAndSecondaries(ChunkHandle handle);

	// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
	std::tuple<GFSError, std::vector<std::string> /*Locations*/>
		RPCGetReplicas(ChunkHandle handle);

	// RPCGetFileInfo is called by client to get file information
	std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/>
		RPCGetFileInfo(std::string path);

	// RPCCreateFile is called by client to create a new file
	GFSError
		RPCCreateFile(std::string path);

	// RPCCreateFile is called by client to delete a file
	GFSError
		RPCDeleteFile(std::string path);

	// RPCMkdir is called by client to make a new directory
	GFSError
		RPCMkdir(std::string path);

	// RPCListFile is called by client to get the file list
	std::tuple<GFSError, std::vector<std::string> /*FileNames*/>
		RPCListFile(std::string path);

	// RPCGetChunkHandle returns the chunk handle of (path, index).
	// If the requested index is larger than the number of chunks of this path by exactly one, create one.
	std::tuple<GFSError, ChunkHandle>
		RPCGetChunkHandle(std::string path, std::uint64_t chunkIndex);

protected:
	LightDS::Service &srv;
	std::string rootDir;
};

#endif