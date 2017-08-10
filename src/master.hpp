#ifndef GFS_MASTER_HPP
#define GFS_MASTER_HPP

#include "common.hpp"
#include <string>
#include <service.hpp>
#include <vector>
#include <mutex>
#include <deque>
#include <algorithm>
#include <fstream>
#include "metadata.hpp"
#include <map>
#include "chunkserver.hpp"

class Master
{
protected:
	LightDS::Service &srv;
	std::string rootDir;
	bool running;
	
public:
	ChunkHandle chunkHandleAlloc;
	uint64_t chunkCnt;
	const time_t delFileSaveTime = 60;
	const time_t leaseTime = 60;
	const uint64_t replicaNum = 3;
	const time_t checkTime = 180;
	const time_t serverDeadTime = 60;
	
	void bind(std::string rpcName, Ret(*func)(Args...))
	{
		srv.RPCBind(rpcName, std::function<Ret(Args...)>([this, func](Args ...args) -> Ret 
		{
			return (this->*func)(std::forward<Args>(args)...);
		}));
	}
	
	Master(LightDS::Service &srv, const std::string &rootDir)
	{
		this->rootDir = rootDir;	
		this->srv = srv;
		chunkHandleAlloc = 1;
		chunkCnt = 0;
		
		bind("RPCCreateFile", &RPCCreateFile);
		bind("RPCDeleteFile", &RPCDeleteFile);
		bind("RPCGetChunkHandle", &RPCGetChunkHandle);
		bind("RPCGetFileInfo", &RPCGetFileInfo);
		bind("RPCGetPrimaryAndSecondaries", &RPCGetPrimaryAndSecondaries);
		bind("RPCGetReplicas", &RPCGetReplicas);
		bind("RPCHeartbeat", &RPCHeartbeat);
		bind("RPCListFile", &RPCListFile);
		bind("RPCMkdir", &RPCMkdir);
		bind("RPCGetChunkHandleForAppend", &RPCGetChunkHandleForAppend);
	}
	void Start()
	{
		running = 1;
		pathToFileMeta.clear();
		pathToServerMeta.clear();
		handleToChunkMeta.clear();
		
		ifstream flog("MasterCheckpoint.log");
		if(flog)
		{
			flog >> chunkHandleAlloc >> chunkCnt;
			flog >> treeCnt;
			std::string input;
			for(int i = 0; i < treeCnt; i++)
			{
				flog >> input;
				RPCMkdir(input);
			}
			
			int ss;
			flog >> ss;
			for(int i = 0; i < ss; i++)
			{
				FileMeta* curFM = new FileMeta;	
				int sss;
				flog >> sss;
				std::string in;
				std::deque<std::string> tmp;
				for(int j = 0; j < sss; j++)
				{
					flog >> in;
					tmp.push_back(in);
				}
				flog >> curFM->handle >> curFM->deleted >> curFM->delTimeStamp;
				pathToFileMeta[tmp] = curFM;
			}
			
			flog >> ss;
			std::string hd;
			for(int i = 0; i < ss; i++)
			{
				flog >> hd;
				ChunkMeta* curCM = new ChunkMeta;
				flog >> curCM->handle >> curCM->preHandle >> curCM->nexHandle;
				flog >> sss;
				for(int j = 0; j < sss; j++)
				{
					std::string tmp;
					flog >> tmp;
					curCM->atServer.push_back(tmp);
				}
				flog >> curCM->mainChunk;
				if(curCM->mainChunk == "NAN")
					curCM->mainChunk = "";
				flog >> curCM->expireTimeStamp >> curCM->version;
				handleToChunkMeta[hd] = curCM;
			}
			
			flog >> ss;
			std::string path;
			for(int i = 0; i < ss; i++)
			{
				flog >> path;	
				int sss;
				flog >> sss;
				ServerMeta* curSM = new ServerMeta;
				for(int j = 0; j < sss; j++)
				{
					ChunkHandle hd;	
					flog >> hd;
					curSM->handleList.push_back(hd);
				}
				flog >> curSM->lastHeartBeat;
				pathToServerMeta[path] = curSM;
			}
			
			flog.close();
		}
	}
	void Shutdown()
	{
		running = 0;
		ofstream flog;
		flog.open("MasterCheckpoint.log", ofstream::trunc);
		flog << chunkHandleAlloc << endl << chunkCnt << endl << endl;
		//DirTree
		flog << treeCnt << endl;
		std::deque<DirectoryTree::Node*> array;
		array.push_back(dirTree.root);
		while(array.size())
		{
			DirectoryTree::Node* now = array.pop_front();	
			std::deque<std::string> output = DirectoryTree::getPath(now);
			for(int j = 0; j < output.size(); j++)
				flog << output[j];
			for(int j = 0; j < now->inferior.size(); j++)
			{
				array.push_back(now->inferior[j]);	
			}
			flog << endl;
		}
		flog << endl;
		
		//FileMeta
		flog << pathToFileMeta.size() << endl;
		std::map<std::deque<std::string>, FileMeta*>::iterator it;
		for(it = pathToFileMeta.begin(); it != pathToFileMeta.end(); it++)
		{
			flog << it->first.size() << " ";
			for(int j = 0; j < it->first.size(); j++)
				flog << it->first[j];
			flog << " " << it->second->handle << " " << (int)it->second->deleted << " " << it->second->delTimeStamp << endl;	
		}
		
		//ChunkMeta
		flog << handleToChunkMeta.size() << endl;
		std::map<ChunkHandle, ChunkMeta*>::iterator it;
		for(it = handleToChunkMeta.begin(); it != handleToChunkMeta.end(); it++)
		{
			flog << it->first << " ";
			flog << it->second->handle << " " << it->second->preHandle << " " << it->second->nexHandle << " ";
			flog << it->second->atServer.size() << " ";
			for(int j = 0; j < it->second->atServer.size(); j++)
				flog << it->second->atServer[j] << " ";
			if(it->second->mainChunk != "")
				flog << it->second->mainChunk;
			else
				flog << "NAN"; 
			flog << " " << it->second->expireTimeStamp << " " << it->second->version << endl;
		}
		
		//ServerMeta
		flog << pathToServerMeta.size() << endl;
		std::map<std::string, ServerMeta*>::iterator it;
		for(it = pathToServerMeta.begin(); it != pathToServerMeta.end(); it++)
		{
			flog << it->first << " ";	
			flog << it->second->handleList.size() << " ";
			for(int j = 0; j < it->second->handleList.size(); j++)
				flog << it->second->handleList[j] << " ";
			flog << it->second->lastHeartBeat << endl;
		}
		
		flog.close();
	}

	class DirectoryTree
	{
	public:	
		class Node
		{
		public:
			std::string dirName;
			Node* superior;
			std::vector<Node*> inferior;
	
			Node()
			{
				superior = NULL;
				inferior.clear();	
			}
		};	
		Node* root;
		DirectoryTree()	
		{
			root = new Node();
			root->dirName = rootDir;
		}
		std::deque<std::string> getPath(Node* nd)
		{
			std::deque<std::string> path;
			Node* now = nd;
			do
			{
				path.push_front(now->dirName);
				now = now->superior;
			}while(path.front() != root->dirName);
			return path;
		}
		/*void printTree()
		{
			Node* array[1000];
			int st = 0, ed = 0;	
			array[ed++] = root;
			while(st < ed)
			{
				Node* now = array[st++];
				std::cout << now->dirName << " : ";
				for(int j = 0; j < now->inferior.size(); j++)
				{
					std::cout << now->inferior[j]->dirName << " ";
					array[ed++] = now->inferior[j];
				}
				std::cout << "\n";
			}
			std::cout << "OVER\n";
		}*/
		void deleteNode(Node* now)
		{
			for(int i = 0; i < now->inferior.size(); i++)
				deleteNode(now->inferior[i]);
			delete now;
		}
		~DirectoryTree()
		{
			deleteNode(root);	
		}
	}dirTree;
	
	std::deque<std::string> turnPathIntoDeque(const std::string & str)
	{
		std::deque<std::string> path;
		int last = 0, now = 1;
		int len = str.length();
		while(true)
		{
			if(now == len - 1)
			{
				path.push_back(str.substr(last));
				break;	
			}
			if(str[now] == '/')
			{
				path.push_back(str.substr(last, now - last));
				last = now;
			}
			now++;
		}
		return path;
	}
	
	DirectoryTree::Node* findNode(std::string path)
	{
		std::deque<std::string> pathDeque = turnPathIntoDeque(path);	
		DirectoryTree::Node* now = dirTree.root;
		for(int i = 1; i < pathDeque.size(); i++)
		{
			for(int j = 0; j < now->inferior.size(); j++)
			{
				if(now->inferior[j]->dirName == pathDeque[i])
				{
					now = now->inferior[j];
					break;
				}
			}
		}
		return now;
	}
	
	std::map<std::deque<std::string>, FileMeta*> pathToFileMeta;
	std::map<std::string, ServerMeta*> pathToServerMeta;
	std::map<ChunkHandle, ChunkMeta*> handleToChunkMeta;
	
	
	// FINISHED
	// BackgroundActivity does all the background activities:
	// dead chunkserver handling, garbage collection, stale replica detection, etc
	void BackgroundActivity()
	{
		while(true)
		{
			if(running == 0)
				break;
			time_t tt = time(NULL);
			if(time(NULL) % checkTime == 0)
			{
				for(int i = 0; i < srv.ListService("chunkserver").size(); i++)//dead server & garbage
				{
					std::string serverID = srv.ListService("chunkserver")[i];
					if(tt - pathToServerMeta[serverID]->lastHeartBeat > serverDeathTime)
					{
						for(int j = 0; j < pathToServerMeta[serverID]->handleList.size(); j++)
						{
							ChunkHandle hd = pathToServerMeta[serverID]->handleList[i];
							ChunkMeta* curCM = handleToChunkMeta[hd];
							if(mainChunk == serverID)
							{
								mainChunk == "";
								expireTimeStamp = 0;	
							}
							std::deque<std::string> updateAtServer;
							for(int k = 0; k < curCM->atServer.size(); k++)
							{
								if(curCM->atServer[k] != serverID)
									updateAtServer.push_back(curCM->atServer[k]);	
							}
							curCM->atServer.clear();
							for(int k = 0; k < updateAtServer.size(); k++)
							{
								curCM->atServer.push_back(updateAtServer[k]);	
							}
							int gs = srv.ListService("chunkserver").size();
							for(int k = 0; k < gs; k++)
							{
								if(srv.ListService("chunkserver")[k] != serverID)
								{
									srv.RPCCall(srv.ListService("chunkserver")[k], "RPCDeleteChunk", serverID, hd);
								}
							}
						}
					}
				}
			
				std::map<ChunkHandle>::iterator it;
				for(it = handleToChunkMeta.begin(); it != handleToChunkMeta.end(); it++)
				{
					ChunkMeta* curCM = *it;	
					if(curCM->atServer.size() < replicaNum)
					{
						int fs = replicaNum - curCM->atServer.size();
						for(int repTime = 1; repTime <= fs; repTime++)
						{
							for(int j = 0; j < srv.ListService("chunkserver").size(); j++)
							{
								std::string thisID = srv.ListService("chunkserver")[j];
								bool flag = 1;
								for(int k = 0; k < curCM->atServer.size(); k++)
								{
									if(thisID == curCM->atServer[k])
									{
										flag = 0;
										break;
									}
								}
								if(flag)
								{
									GFSError mid = srv.RPCCall(curCM->atServer[0], "RPCSendCopy", curCM->handle, thisID);
									curCM->atServer.push_back(thisID);
									pathToServerMeta[thisID]->handleList.push_back(curCM->handle);
									if(mid.errCode != GFSErrorCode::OK)
										repTime--;
									break;	
								}
							}
						}
					}
				}
			}
		
			std::map<std::deque<std::string>, FileMeta*>::iterator it;
			for(it = pathToFileMeta.begin(); it != pathToFileMeta.end(); it++)
			{
				FileMeta* curFM = (*it);
				int subCnt = 0;
				if(tt > curFM->delTimeStamp)
				{
					DirectoryTree::Node* delNode = findNode(it->first);
					DirectoryTree::Node* father = delNode->superior;
					std::vector<DirectoryTree::Node*> updateInferior;
					for(int i = 0; i < father->inferior.size(); i++)
					{
						if(father->inferior[i] != delNode)
							updateInferior.push_back(father->inferior[i]);
					}
					delete delNode;
					father->inferior.clear();
					for(int i = 0; i < updateInferior.size(); i++)
						father->inferior.push_back(updateInferior[i]);
					ChunkMeta* curCM = handleToChunkMeta[curFM->handle];
					while(true)
					{
						ChunkHandle nexHd = curCM->nexHandle;
						for(int i = 0; i < curCM->atServer.size(); i++)
						{
							std::string serverID = curCM->atServer[i];
							std::deque<ChunkHandle> updateHandleList;	
							ServerMeta* curSM = pathToServerMeta[serverID];
							for(int j = 0; j < curSM->handleList.size(); j++)
							{
								if(curSM->handleList[j] != curFM->handle)	
									updateHandleList.push_back(curSM->handleList[j]);
							}
							curSM->handleList.clear();
							for(int j = 0; j < updateHandleList.size(); j++)
							{
								curSM->handleList.push_back(updateHandleList[j]);	
							}
							srv.RPCCall(serverID, "RPCDeleteChunk", serverID, curCM->handle);
						}
						delete curCM;
						treeCnt--;
						subCnt++;
						if(nexHd == 0)
							break;
						curCM = handleToChunkMeta[nexHd];
					}
					chunkCnt -= subCnt;
				}
			}
		}
	}

	// FINISHED
	// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
	std::tuple<GFSError, std::vector<ChunkHandle> /*Garbage Chunks*/>
		RPCHeartbeat(std::vector<ChunkHandle> leaseExtensions, std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks, std::vector<ChunkHandle> failedChunks)
		{
			std::string serverID = srv.getRPCCaller();
			time_t tt = time(NULL);
			pathToServerMeta[serverID]->lastHeartBeat = tt;
			
			std::vector<ChunkHandle> garbageChunks;
			ServerMeta* curSM = pathToServerMeta[serverID];
			curSM->handleList.clear();
			for(int i = 0; i < chunks.size(); i++)
			{
				ChunkMeta* curCM = handleToChunkMeta[chunks[i].get<0>];
				if(curCM->version > chunks[i].get<1>)
				{
					garbageChunks.push_back(chunks[i].get<0>);
				}
				else
				{
					curCM->version = chunks[i].get<1>;
					curSM->handleList.push_back(chunks[i].get<0>);	
				}
			}
			
			time_t tt = time(NULL);
			std::vector<std::tuple<ChunkHandle, ChunkVersion, std::uint64_t>> vct;
			for(int i = 0; i < leaseExtensions.size(); i++)
			{
				ChunkMeta* curCM = handleToChunkMeta[leaseExtensions[i]];	
				curCM->expireTimeStamp = tt + leaseTime;
				curCM->version++;
				vct.push_back(make_tuple(leaseExtensions[i], curCM->version, curCM->expireTimeStamp));
			}
			
			std::deque<std::string> updateAtServer;
			for(int i = 0; i < failedChunks.size(); i++)
			{
				ChunkHandle hd = failedChunks[i];
				ChunkMeta* curCM = handleToChunkMeta[hd];
				for(int j = 0; j < curCM->atServer.size(); j++)
				{
					if(curCM->atServer[j] != serverID)	
						updateAtServer.push_back(curCM->atServer[j]);
				}
				curCM->atServer.clear();
				for(int j = 0; j < updateAtServer.size(); j++)
					curCM->atServer.push_back(updateAtServer[j]);
				garbageChunks.push_back(failedChunks[i]);
			}
			return make_tuple(GFSError(GFSErrorCode::OK), garbageChunks);
		}

	// FINISHED
	// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
	// If no one holds the lease currently, grant one.
	std::tuple<GFSError, std::string /*Primary Address*/, std::vector<std::string> /*Secondary Addresses*/, std::uint64_t /*Expire Timestamp*/>
		RPCGetPrimaryAndSecondaries(ChunkHandle handle)
		{
			if(!handleToChunkMeta.count(handle))
			{
				GFSError Csq(GFSErrorCode::invalidHandle);
				std::vector<std::string> ans;
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = make_tuple(Csq, "", ans, 0);
				return Rtn;
			}
			ChunkMeta* curCM = handleToChunkMeta[handle];
			if(curCM->mainChunk != "")
			{
				std::string primary = curCM->mainChunk;
				std::vector<std::string> secondaries;
				for(int i = 0; i < curCM->atServer.size(); i++)
				{
					if(curCM->atServer[i] != primary)
					{
						secondaries.push_back(curCM->atServer[i]);
					}
				}
				GFSError Csq(GFSErrorCode::OK);
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = make_tuple(Csq, primary, secondaries, curCM->expireTimeStamp);
				return Rtn;
			}
			else
			{
				std::string primary = curCM->atServer[0];
				curCM->expireTimeStamp = time(NULL) + leaseTime;
				curCM->version++;
				std::vector<std::tuple<ChunkHandle, ChunkVersion, std::uint64_t>> vct;
				std::tuple<ChunkHandle, ChunkVersion, std::uint64_t> tmp = make_tuple(handle, curCM->version, curCM->expireTimeStamp);
				vct.push_back(tmp);
				GFSError mid = srv.RPCCall(primary, "RPCGrantLease", vct);
				if(mid.errCode != GFSErrorCode::OK)
				{
					std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = make_tuple(mid, "", std::vector<std::string>(), 0);
					return Rtn;	
				}
				GFSError mid = srv.RPCCall(primary, "RPCUpdateVersion", handle, curCM->version);
				if(mid.errCode != GFSErrorCode::OK)
				{
					std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = make_tuple(mid, "", std::vector<std::string>(), 0);
					return Rtn;	
				}
				std::vector<std::string> secondaries;
				for(int i = 1; i < curCM->atServer.size(); i++)
				{
					secondaries.push_back(curCM->atServer[i]);
					GFSError mid = srv.RPCCall(curCM->atServer[i], "RPCUpdateVersion", curCM->version);
					if(mid.errCode != GFSErrorCode::OK)
					{
						std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = make_tuple(mid, "", std::vector<std::string>(), 0);
						return Rtn;	
					}
				}
				GFSError Csq(GFSErrorCode::OK);
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = make_tuple(Csq, primary, secondaries, curCM->expireTimeStamp);
				return Rtn;
			}
		}

	// FINISHED
	// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
	std::tuple<GFSError, std::vector<std::string> /*Locations*/>
		RPCGetReplicas(ChunkHandle handle)
		{
			if(!handleToChunkMeta.count(handle))
			{
				GFSError Csq(GFSErrorCode::invalidHandle);
				std::vector<std::string> ans;
				std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, ans);
				return Rtn;
			}
			std::vector<std::string> ans;
			for(int i = 0; i < handleToChunkMeta[handle]->atServer.size(); i++)
			{
				ans.push_back(handleToChunkMeta[handle]->atServer[i]);	
			}
			GFSError Csq(GFSErrorCode::OK);
			std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, ans);
			return Rtn;
		}
	
	// FINISHED
	// RPCGetFileInfo is called by client to get file information
	std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/>
		RPCGetFileInfo(std::string path)
		{
			bool isDir;
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);	
			if(pathToFileMeta.count(pathDeque))
				isDir = 0;
			else
				isDir = 1;
			ChunkMeta* curCM = pathToFileMeta[pathDeque];
			uint64_t cnt = 1;
			while(curCM->nexChunk != NULL)
			{
				cnt++;	
				curCM = handleToChunkMeta[curCM->nexHandle];
			}
			return make_tuple(GFSError(GFSErrorCode::OK), isDir, 0, cnt);
		}

	// FINISHED
	// RPCCreateFile is called by client to create a new file
	GFSError
		RPCCreateFile(std::string path)
		{
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);	
			GFSError middle = RPCMkdir(path);
			if(middle.errCode != GFSErrorCode::OK)
				return middle;	
			treeCnt++;
			FileMeta* curFM = new FileMeta;
			ChunkHandle newHandle = chunkHandleAlloc++;
			chunkCnt++;
			pathToFileMeta[pathDeque] = curFM;
			ChunkMeta* curCM = new ChunkMeta;
			curCM->handle = newHandle;
			for(int i = 0; i < serverList.size(); i++)
			{
				if(pathToServerMeta[serverList[i]]->handleList.size() <= chunkCnt / serverList.size())
				{
					GFSError mid = srv.RPCCall(serverList[i], "RPCCreateChunk", newHandle);
					if(mid.errCode == GFSErrorCode::failedChunkCreation)
						return mid;
					handleToChunkMeta[newHandle] = curCM;
					tmp->atServer.push_back(serverList[i]);
					pathToServerMeta[serverList[i]]->handleList.push_back(newHandle);
					GFSError Csq(GFSErrorCode::OK);
					return Csq;
				}
			}
		}
		
	// FINISHED
	// RPCCreateFile is called by client to delete a file
	GFSError
		RPCDeleteFile(std::string path)
		{
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);	
			if(!pathToFileMeta.count(pathDeque))
			{
				GFSError Csq;
				Csq.errCode = GFSErrorCode::invalidPath;
				return Csq;
			}
			FileMeta* curFM = pathToFileMeta[pathDeque];
			curFM->deleted = 1;
			delTimeStamp = time(NULL) + delFileSaveTime;
			GFSError Csq;
			Csq.errCode = GFSErrorCode::OK;
			return Csq;
		}

	// FINISHED
	// RPCMkdir is called by client to make a new directory
	GFSError
		RPCMkdir(std::string path)
		{
			GFSError Csq;
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);
			DirectoryTree::Node* now = dirTree.root;
			int curPath = 1;
			if(pathDeque[0] != dirTree.root->dirName)
			{
				Csq.errCode = GFSErrorCode::invalidPath;
				return Csq;
			}
			if(pathDeque.size() == 1)
			{
				Csq.errCode = GFSErrorCode::pathAlreadyExist;
				return Csq;
			}
			while(true)
			{
				for(int i = 0; i < now->inferior.size(); i++)
				{
					if(now->inferior[i]->dirName == pathDeque[curPath])
					{
						if(curPath == pathDeque.size() - 1)
						{
							Csq.errCode = GFSErrorCode::pathAlreadyExist;
							return Csq;	
						}
						now = now->inferior[i];
						curPath++;
						continue;
					}	
				}
				if(curPath == pathDeque.size() - 1)
				{
					DirectoryTree::Node* newNode = new DirectoryTree::Node();
					newNode->dirName = pathDeque[pathDeque.size() - 1];
					newNode->superior = now;
					now->inferior.push_back(newNode);
					Csq.errCode = GFSErrorCode::OK;
					
					return Csq;
				}	
				else
				{
					Csq.errCode = GFSErrorCode::invalidPath;
					return Csq;
				}
			}
		}
		
	// FINISHED
	// RPCListFile is called by client to get the file list
	std::tuple<GFSError, std::vector<std::string> /*FileNames*/>
		RPCListFile(std::string path)
		{
			GFSError Csq;
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);
			DirectoryTree::Node* now = dirTree.root;
			int curPath = 1;
			if(pathDeque[0] != dirTree.root->dirName)
			{
				Csq.errCode = GFSErrorCode::invalidPath;
				std::vector<std::string> tmp;
				std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, tmp);
				return Rtn;
			}
			if(pathDeque.size() == 1)
			{
				std::vector<std::string> ans;
				for(int i = 0; i < now->inferior.size(); i++)
					ans.push_back(now->inferior[i]->dirName);
				std::sort(ans.begin(), ans.end());
				Csq.errCode = GFSErrorCode::OK;
				std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, ans);
				return Rtn;
			}
			while(true)
			{
				for(int i = 0; i < now->inferior.size(); i++)
				{
					if(now->inferior[i]->dirName == pathDeque[curPath])
					{
						if(curPath == pathDeque.size() - 1)
						{
							now = now->inferior[i];
							std::vector<std::string> ans;
							for(int i = 0; i < now->inferior.size(); i++)
								ans.push_back(now->inferior[i]->dirName);
							std::sort(ans.begin(), ans.end());
							Csq.errCode = GFSErrorCode::OK;
							std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, ans);
							return Rtn;
						}
						now = now->inferior[i];
						curPath++;
						continue;
					}	
				}
				std::vector<std::string> tmp;
				Csq.errCode = invalidPath;
				std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, tmp);
				return Rtn;
			}
		};
		
	int	RPCGetChunkHandleForAppend(std::path)
	{
		int ans = 0;
		std::deque<std::string> pathDeque = turnPathIntoDeque(path);
		FileMeta* curFM = pathToFileMeta[pathDeque];
		ChunkMeta* curCM = handleToChunkMeta[curFM->handle];
		while(curCM->nexChunk != 0)
		{
			curCM = handleToChunkMeta[curCM->nexHandle];
			ans++;
		}
		return ans;
	}

	// FINISHED
	// RPCGetChunkHandle returns the chunk handle of (path, index).
	// If the requested index is larger than the number of chunks of this path by exactly one, create one.
	std::tuple<GFSError, ChunkHandle>
		RPCGetChunkHandle(std::string path, std::uint64_t chunkIndex);
		{
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);
			if(pathToFileMeta.count(pathDeque))
			{
				FileMeta* curFM = pathToFileMeta[path];
				if(curFM->deleted)
				{
					GFSError Csq;
					Csq.errCode = GFSErrorCode::fileDeleted;
					std::tuple<GFSError, CHunkHandle> Rtn = make_tuple(Csq, 0);
					return Rtn;	
				}
				ChunkMeta* curCM = handleToChunkMeta[curFM->handle];
				for(int i = 0; i < chunkIndex; i++)
				{
					ChunkHandle tmp = curCM->nexHandle;
					if(tmp == -1)
					{
						GFSError Csq;
						Csq.errCode = GFSErrorCode::indexOutOfBound;
						std::tuple<GFSError, ChunkHandle> Rtn = make_tuple(Csq, -1);	
						return Rtn;
					}
					curCM = handleToChunkMeta[tmp];
				}
				ChunkHandle tmp = curCM->nexHandle;
				if(tmp == -1)
				{
					ChunkHandle newHandle = chunkHandleAlloc++;
					chunkCnt++;
					ChunkMeta* tmp = new ChunkMeta;
					tmp->preHandle = curCM->handle;
					curCM->nexHandle = newHandle;
					tmp->handle = newHandle;
					for(int i = 0; i < serverList.size(); i++)
					{
						if(pathToServerMeta[serverList[i]]->handleList.size() <= chunkCnt / serverList.size())
						{
							GFSError mid = srv.RPCCall(serverList[i], "RPCCreateChunk", newHandle);
							if(mid.errCode == GFSErrorCode::failedChunkCreation)
							{
								std::tuple<GFSError, ChunkHandle> Rtn = make_tuple(mid, 0);
								return Rtn;	
							}
							handleToChunkMeta[newHandle] = tmp;
							tmp->atServer.push_back(serverList[i]);
							pathToServerMeta[serverList[i]]->handleList.push_back(newHandle);
							break;
						}
					}
					GFSError Csq;
					Csq.errCode = GFSErrorCode::OK;
					std::tuple<GFSError, ChunkHandle> Rtn = make_tuple(Csq, newHandle);
					return Rtn;
				}
				else
				{
					GFSError Csq;
					Csq.errCode = GFSErrorCode::OK;
					std::tuple<GFSError, ChunkHandle> Rtn = make_tuple(Csq, tmp);
					return Rtn;	
				}
			}
			else
			{
				GFSError Csq;
				Csq.errCode = GFSErrorCode::invalidPath;
				std::tuple<GFSError, ChunkHandle> Rtn = make_tuple(Csq, 0);
				return Rtn;
			}
		}
};

#endif
