#ifndef GFS_COMMON_H
#define GFS_COMMON_H

#include <msgpack.hpp>

typedef std::uint64_t ChunkHandle;
typedef std::uint64_t ChunkVersion;

enum class GFSErrorCode : std::uint32_t
{
	OK = 0,
	invalidPath = 1;
	pathAlreadyExist = 2;
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
