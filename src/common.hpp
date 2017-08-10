#ifndef GFS_COMMON_HPP
#define GFS_COMMON_HPP

#include <msgpack.hpp>

typedef std::uint64_t ChunkHandle;
typedef std::uint64_t ChunkVersion;

enum class GFSErrorCode : std::uint32_t
{
	OK = 0,
	invalidPath = 1,
	pathAlreadyExist = 2,
	fileDeleted = 3,
	indexOutOfBound = 4,
	dataIDUsed = 5,
	invalidHandle = 6,
	failedChunkCreation = 7,
	wrongSerialNo = 8,
	retryAppend = 9,
};

struct GFSError
{
	GFSErrorCode errCode;
	std::string description;

	GFSError(GFSErrorCode a = GFSErrorCode::OK, std::string b = "")
	{
		errCode = a;
		description = b;	
	}
	MSGPACK_DEFINE(errCode, description);
};

MSGPACK_ADD_ENUM(GFSErrorCode);

#endif
