#ifndef GFS_MASTER_HPP
#define GFS_MASTER_HPP

#include "common.hpp"
#include <string>
#include <service.hpp>
#include <vector>
#include <mutex>
#include <deque>
#include <tuple>
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
	uint64_t treeCnt;
	
	Master(LightDS::Service &srv, const std::string &root) : srv(srv), rootDir(root)
	{
		chunkHandleAlloc = 1;
		chunkCnt = 0;
		treeCnt = 0;
		dirTree.root->dirName = "/";
		
		srv.RPCBind<std::tuple<GFSError, std::vector<ChunkHandle>>(std::vector<ChunkHandle>, std::vector<std::tuple<ChunkHandle, ChunkVersion>>, std::vector<ChunkHandle>)>("RPCHeartbeat", std::bind(&Master::RPCHeartbeat, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
		srv.RPCBind<std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t>(ChunkHandle)>("RPCGetPrimaryAndSecondaries", std::bind(&Master::RPCGetPrimaryAndSecondaries, this, std::placeholders::_1));
		srv.RPCBind<std::tuple<GFSError, std::vector<std::string>>(ChunkHandle)>("RPCGetReplicas", std::bind(&Master::RPCGetReplicas, this, std::placeholders::_1));
		srv.RPCBind<std::tuple<GFSError, bool, std::uint64_t, std::uint64_t>(std::string)>("RPCGetFileInfo", std::bind(&Master::RPCGetFileInfo, this, std::placeholders::_1));
		srv.RPCBind<GFSError(std::string)>("RPCCreateFile", std::bind(&Master::RPCCreateFile, this, std::placeholders::_1));
		srv.RPCBind<GFSError(std::string)>("RPCDeleteFile", std::bind(&Master::RPCDeleteFile, this, std::placeholders::_1));
		srv.RPCBind<GFSError(std::string)>("RPCMkdir", std::bind(&Master::RPCMkdir, this, std::placeholders::_1));
		srv.RPCBind<std::tuple<GFSError, std::vector<std::string>>(std::string)>("RPCListFile", std::bind(&Master::RPCListFile, this, std::placeholders::_1));
		srv.RPCBind<int(std::string)>("RPCGetChunkHandleForAppend", std::bind(&Master::RPCGetChunkHandleForAppend, this, std::placeholders::_1));
		srv.RPCBind<std::tuple<GFSError, ChunkHandle>(std::string, std::uint64_t)>("RPCGetChunkHandle", std::bind(&Master::RPCGetChunkHandle, this, std::placeholders::_1, std::placeholders::_2));
		
		/*for(int i = 0; i < srv.ListService("chunkserver").size(); i++)
		{
			std::cerr << "hey\n";
			ServerMeta* curSM = new ServerMeta;
			pathToServerMeta[srv.ListService("chunkserver")[i].to_string()] = curSM;
		}*/
	
	}
	std::thread threadBG;
	void Start()
	{
		std::cerr << "Master start\n";
		running = 1;
		pathToFileMeta.clear();
		pathToServerMeta.clear();
		handleToChunkMeta.clear();
		threadBG = std::thread(BackgroundActivity);
		
		std::ifstream flog("MasterCheckpoint.log");
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
			ChunkHandle hd;
			for(int i = 0; i < ss; i++)
			{
				flog >> hd;
				ChunkMeta* curCM = new ChunkMeta;
				flog >> curCM->handle >> curCM->preHandle >> curCM->nexHandle;
				int sss;
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
		{
			std::lock_guard<std::mutex> lock(mtxStop);
			stop = true;
			cvStop.notify_all();
		}
		
		threadBG.join();
		
		running = 0;
		std::ofstream flog;
		flog.open("MasterCheckpoint.log", std::ofstream::trunc);
		flog << chunkHandleAlloc << std::endl << chunkCnt << std::endl << std::endl;
		//DirTree
		flog << treeCnt << std::endl;
		std::deque<DirectoryTree::Node*> array;
		array.push_back(dirTree.root);
		while(array.size())
		{
			DirectoryTree::Node* now = array[0];
			array.pop_front();	
			std::deque<std::string> output = dirTree.getPath(now);
			for(int j = 0; j < output.size(); j++)
				flog << output[j];
			for(int j = 0; j < now->inferior.size(); j++)
			{
				array.push_back(now->inferior[j]);	
			}
			flog << std::endl;
		}
		flog << std::endl;
		
		//FileMeta
		flog << pathToFileMeta.size() << std::endl;
		std::map<std::deque<std::string>, FileMeta*>::iterator it;
		for(it = pathToFileMeta.begin(); it != pathToFileMeta.end(); it++)
		{
			flog << it->first.size() << " ";
			for(int j = 0; j < it->first.size(); j++)
				flog << it->first[j];
			flog << " " << it->second->handle << " " << (int)it->second->deleted << " " << it->second->delTimeStamp << std::endl;	
		}
		
		//ChunkMeta
		flog << handleToChunkMeta.size() << std::endl;
		std::map<ChunkHandle, ChunkMeta*>::iterator it1;
		for(it1 = handleToChunkMeta.begin(); it1 != handleToChunkMeta.end(); it1++)
		{
			flog << it1->first << " ";
			flog << it1->second->handle << " " << it1->second->preHandle << " " << it1->second->nexHandle << " ";
			flog << it1->second->atServer.size() << " ";
			for(int j = 0; j < it1->second->atServer.size(); j++)
				flog << it1->second->atServer[j] << " ";
			if(it1->second->mainChunk != "")
				flog << it1->second->mainChunk;
			else
				flog << "NAN"; 
			flog << " " << it1->second->expireTimeStamp << " " << it1->second->version << std::endl;
		}
		
		//ServerMeta
		flog << pathToServerMeta.size() << std::endl;
		std::map<std::string, ServerMeta*>::iterator it2;
		for(it2 = pathToServerMeta.begin(); it2 != pathToServerMeta.end(); it2++)
		{
			flog << it2->first << " ";	
			flog << it2->second->handleList.size() << " ";
			for(int j = 0; j < it2->second->handleList.size(); j++)
				flog << it2->second->handleList[j] << " ";
			flog << it2->second->lastHeartBeat << std::endl;
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
		int last = 1, now = 1;
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
				last = now + 1;
			}
			now++;
		}
		return path;
	}
	
	DirectoryTree::Node* findNode(std::deque<std::string> pathDeque)
	{	
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
	
	std::mutex mtxStop;
	std::condition_variable cvStop;
	bool stop;
	// FINISHED
	// BackgroundActivity does all the background activities:
	// dead chunkserver handling, garbage collection, stale replica detection, etc
	void BackgroundActivity()
	{
		using namespace std::chrono_literals;
		std::unique_lock<std::mutex> lock(mtxStop);
		for(; !stop; cvStop.wait_for(lock, 1s))
		{
			if(running == 0)
				break;
			time_t tt = time(NULL);
				for(int i = 0; i < srv.ListService("chunkserver").size(); i++)//dead server & garbage
				{
					std::string serverID = srv.ListService("chunkserver")[i].to_string();
					if(tt - pathToServerMeta[serverID]->lastHeartBeat > serverDeadTime)
					{
						for(int j = 0; j < pathToServerMeta[serverID]->handleList.size(); j++)
						{
							ChunkHandle hd = pathToServerMeta[serverID]->handleList[i];
							ChunkMeta* curCM = handleToChunkMeta[hd];
							if(curCM->mainChunk == serverID)
							{
								curCM->mainChunk == "";
								curCM->expireTimeStamp = 0;	
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
								if(srv.ListService("chunkserver")[k].to_string() != serverID)
								{
									srv.RPCCall(srv.ListService("chunkserver")[k], "RPCDeleteChunk", serverID, hd);
								}
							}
						}
					}
				}
			
				std::map<ChunkHandle, ChunkMeta*>::iterator it;
				for(it = handleToChunkMeta.begin(); it != handleToChunkMeta.end(); it++)
				{
					ChunkMeta* curCM = it->second;	
					if(curCM->atServer.size() < replicaNum)
					{
						int fs = replicaNum - curCM->atServer.size();
						for(int repTime = 1; repTime <= fs; repTime++)
						{
							for(int j = 0; j < srv.ListService("chunkserver").size(); j++)
							{
								std::string thisID = srv.ListService("chunkserver")[j].to_string();
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
									GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(curCM->atServer[0]), "RPCSendCopy", curCM->handle, thisID).get().as<GFSError>();
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
		
			std::map<std::deque<std::string>, FileMeta*>::iterator it1;
			for(it1 = pathToFileMeta.begin(); it1 != pathToFileMeta.end(); it1++)
			{
				FileMeta* curFM = it1->second;
				int subCnt = 0;
				if(tt > curFM->delTimeStamp)
				{
					DirectoryTree::Node* delNode = findNode(it1->first);
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
							srv.RPCCall(LightDS::User::RPCAddress::from_string(serverID), "RPCDeleteChunk", serverID, curCM->handle);
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
			std::cerr << "HB  "<<srv.getRPCCaller() << std::endl;
			std::string serverID = srv.getRPCCaller();
			time_t tt = time(NULL);
			pathToServerMeta[serverID]->lastHeartBeat = tt;
			
			std::vector<ChunkHandle> garbageChunks;
			ServerMeta* curSM = pathToServerMeta[serverID];
			curSM->handleList.clear();
			for(int i = 0; i < chunks.size(); i++)
			{
				ChunkMeta* curCM = handleToChunkMeta[std::get<0>(chunks[i])];
				if(curCM->version > std::get<1>(chunks[i]))
				{
					garbageChunks.push_back(std::get<0>(chunks[i]));
				}
				else
				{
					curCM->version = std::get<1>(chunks[i]);
					curSM->handleList.push_back(std::get<0>(chunks[i]));	
				}
			}
			
			std::vector<std::tuple<ChunkHandle, ChunkVersion, std::uint64_t>> vct;
			for(int i = 0; i < leaseExtensions.size(); i++)
			{
				ChunkMeta* curCM = handleToChunkMeta[leaseExtensions[i]];	
				curCM->expireTimeStamp = tt + leaseTime;
				curCM->version++;
				vct.push_back(std::make_tuple(leaseExtensions[i], curCM->version, curCM->expireTimeStamp));
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
			return std::make_tuple(GFSError(GFSErrorCode::OK), garbageChunks);
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
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = std::make_tuple(Csq, "", ans, 0);
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
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = std::make_tuple(Csq, primary, secondaries, curCM->expireTimeStamp);
				return Rtn;
			}
			else
			{
				std::string primary = curCM->atServer[0];
				curCM->expireTimeStamp = time(NULL) + leaseTime;
				curCM->version++;
				std::vector<std::tuple<ChunkHandle, ChunkVersion, std::uint64_t>> vct;
				std::tuple<ChunkHandle, ChunkVersion, std::uint64_t> tmp = std::make_tuple(handle, curCM->version, curCM->expireTimeStamp);
				vct.push_back(tmp);
				GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(primary), "RPCGrantLease", vct).get().as<GFSError>();
				if(mid.errCode != GFSErrorCode::OK)
				{
					std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = std::make_tuple(mid, "", std::vector<std::string>(), 0);
					return Rtn;	
				}
				mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(primary), "RPCUpdateVersion", handle, curCM->version).get().as<GFSError>();
				if(mid.errCode != GFSErrorCode::OK)
				{
					std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = std::make_tuple(mid, "", std::vector<std::string>(), 0);
					return Rtn;	
				}
				std::vector<std::string> secondaries;
				for(int i = 1; i < curCM->atServer.size(); i++)
				{
					secondaries.push_back(curCM->atServer[i]);
					GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(curCM->atServer[i]), "RPCUpdateVersion", curCM->version).get().as<GFSError>();
					if(mid.errCode != GFSErrorCode::OK)
					{
						std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = std::make_tuple(mid, "", std::vector<std::string>(), 0);
						return Rtn;	
					}
				}
				GFSError Csq(GFSErrorCode::OK);
				std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t> Rtn = std::make_tuple(Csq, primary, secondaries, curCM->expireTimeStamp);
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
				std::tuple<GFSError, std::vector<std::string>> Rtn = std::make_tuple(Csq, ans);
				return Rtn;
			}
			std::vector<std::string> ans;
			for(int i = 0; i < handleToChunkMeta[handle]->atServer.size(); i++)
			{
				ans.push_back(handleToChunkMeta[handle]->atServer[i]);	
			}
			GFSError Csq(GFSErrorCode::OK);
			std::tuple<GFSError, std::vector<std::string>> Rtn = std::make_tuple(Csq, ans);
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
			ChunkMeta* curCM = handleToChunkMeta[pathToFileMeta[pathDeque]->handle];
			uint64_t cnt = 1;
			while(curCM->nexHandle != 0)
			{
				cnt++;	
				curCM = handleToChunkMeta[curCM->nexHandle];
			}
			return std::make_tuple(GFSError(GFSErrorCode::OK), isDir, 0, cnt);
		}

	// FINISHED
	// RPCCreateFile is called by client to create a new file
	GFSError
		RPCCreateFile(std::string path)
		{
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);	
			GFSError middle = RPCMkdir(path);
			if(middle.errCode != GFSErrorCode::OK)
			{
				std::cerr << "error1\n";
				return middle;	
			}
			treeCnt++;
			FileMeta* curFM = new FileMeta;
			ChunkHandle newHandle = chunkHandleAlloc++;
			chunkCnt++;
			pathToFileMeta[pathDeque] = curFM;
			ChunkMeta* curCM = new ChunkMeta;
			curCM->handle = newHandle;
			std::cerr << "this1\n";
			std::map<std::string, ServerMeta*>::iterator it;
			for(it = pathToServerMeta.begin(); it != pathToServerMeta.end(); it++)				
			{
				std::cerr<< "this2\n";
				if(it->second->handleList.size() <= chunkCnt / pathToServerMeta.size())
				{
					std::cerr<< "this3\n";
					GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(it->first), "RPCCreateChunk", newHandle).get().as<GFSError>();
					if(mid.errCode == GFSErrorCode::failedChunkCreation)
						return mid;
					handleToChunkMeta[newHandle] = curCM;
					curCM->atServer.push_back(it->first);
					it->second->handleList.push_back(newHandle);
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
			curFM->delTimeStamp = time(NULL) + delFileSaveTime;
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
			int curPath = 0;
			while(true)
			{
				bool jump = 0;
				std::cerr << "HERE\n";
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
						jump = 1;
						break;
					}	
				}
				if(jump)
					continue;
				if(curPath == pathDeque.size() - 1)
				{
					std::cerr << "YEAH\n";
					DirectoryTree::Node* newNode = new DirectoryTree::Node();
					std::cerr << "YEAH1\n";
					newNode->dirName = pathDeque[pathDeque.size() - 1];
					std::cerr << "YEAH2\n";
					newNode->superior = now;
					std::cerr << "YEAH3\n";
					now->inferior.push_back(newNode);
					std::cerr << "YEAH4\n";
					Csq.errCode = GFSErrorCode::OK;
					std::cerr << "YEAH5\n";
					return Csq;
				}	
				else
				{
					std::cerr << "WATERLOO\n";
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
				std::tuple<GFSError, std::vector<std::string>> Rtn = std::make_tuple(Csq, tmp);
				return Rtn;
			}
			if(pathDeque.size() == 1)
			{
				std::vector<std::string> ans;
				for(int i = 0; i < now->inferior.size(); i++)
					ans.push_back(now->inferior[i]->dirName);
				std::sort(ans.begin(), ans.end());
				Csq.errCode = GFSErrorCode::OK;
				std::tuple<GFSError, std::vector<std::string>> Rtn = std::make_tuple(Csq, ans);
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
							std::tuple<GFSError, std::vector<std::string>> Rtn = std::make_tuple(Csq, ans);
							return Rtn;
						}
						now = now->inferior[i];
						curPath++;
						continue;
					}	
				}
				std::vector<std::string> tmp;
				Csq.errCode = GFSErrorCode::invalidPath;
				std::tuple<GFSError, std::vector<std::string>> Rtn = std::make_tuple(Csq, tmp);
				return Rtn;
			}
		};
		
	int	RPCGetChunkHandleForAppend(std::string path)
	{
		int ans = 0;
		std::deque<std::string> pathDeque = turnPathIntoDeque(path);
		FileMeta* curFM = pathToFileMeta[pathDeque];
		ChunkMeta* curCM = handleToChunkMeta[curFM->handle];
		while(curCM->nexHandle != 0)
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
		RPCGetChunkHandle(std::string path, std::uint64_t chunkIndex)
		{
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);
			if(pathToFileMeta.count(pathDeque))
			{
				FileMeta* curFM = pathToFileMeta[pathDeque];
				if(curFM->deleted)
				{
					GFSError Csq;
					Csq.errCode = GFSErrorCode::fileDeleted;
					std::tuple<GFSError, ChunkHandle> Rtn = std::make_tuple(Csq, (uint64_t)0);
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
						std::tuple<GFSError, ChunkHandle> Rtn = std::make_tuple(Csq, -1);	
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
					std::map<std::string, ServerMeta*>::iterator it;
					for(it = pathToServerMeta.begin(); it != pathToServerMeta.end(); it++)
					{
						if(it->second->handleList.size() <= chunkCnt / pathToServerMeta.size())
						{
							GFSError mid = srv.RPCCall(LightDS::User::RPCAddress::from_string(it->first), "RPCCreateChunk", newHandle).get().as<GFSError>();
							if(mid.errCode == GFSErrorCode::failedChunkCreation)
							{
								std::tuple<GFSError, ChunkHandle> Rtn = std::make_tuple(mid, (uint64_t)0);
								return Rtn;	
							}
							handleToChunkMeta[newHandle] = tmp;
							tmp->atServer.push_back(it->first);
							it->second->handleList.push_back(newHandle);
							break;
						}
					}
					GFSError Csq;
					Csq.errCode = GFSErrorCode::OK;
					std::tuple<GFSError, ChunkHandle> Rtn = std::make_tuple(Csq, newHandle);
					return Rtn;
				}
				else
				{
					GFSError Csq;
					Csq.errCode = GFSErrorCode::OK;
					std::tuple<GFSError, ChunkHandle> Rtn = std::make_tuple(Csq, tmp);
					return Rtn;	
				}
			}
			else
			{
				GFSError Csq;
				Csq.errCode = GFSErrorCode::invalidPath;
				std::tuple<GFSError, ChunkHandle> Rtn = std::make_tuple(Csq, 0);
				return Rtn;
			}
		}
};

#endif
