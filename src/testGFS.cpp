#include <iostream>
#include <vector>
#include <deque>
#include <mutex>
#include <string>
#include <algorithm>

class DirectoryTree
{
public:	
	class Node
	{
	public:
		std::mutex readLock, writeLock;
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
		//root->dirName = rootDir.substr(1);
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
	void printTree()
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

std::string RPCMkdir(std::string path)
{
	//GFSError Csq;
	std::deque<std::string> pathDeque = turnPathIntoDeque(path);
	DirectoryTree::Node* now = dirTree.root;
	int curPath = 1;
	if(pathDeque[0] != dirTree.root->dirName)
	{
		//Csq.errCode = invalidPath;
		//return Csq;
		return "invalidPath\n";	
	}
	if(pathDeque.size() == 1)
	{
		//Csq.errCode = pathAlreadyExist;
		//return Csq;
		return "pathAlreadyExist\n";
	}
	while(true)
	{
		for(int i = 0; i < now->inferior.size(); i++)
		{
			if(now->inferior[i]->dirName == pathDeque[curPath])
			{
				if(curPath == pathDeque.size() - 1)
				{
					//Csq.errCode = pathAlreadyExist;
					//return Csq;	
					return "pathAlreadyExist\n";
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
			//Csq.errCode = OK;
			//return Csq;
			return "success\n";
		}	
		else
		{
			//Csq.errCode = invalidPath;
			//return Csq;
			return "invalidPath\n";	
		}
	}
}

void	RPCListFile(std::string path)
		{
			//GFSError Csq;
			std::deque<std::string> pathDeque = turnPathIntoDeque(path);
			DirectoryTree::Node* now = dirTree.root;
			int curPath = 1;
			if(pathDeque[0] != dirTree.root->dirName)
			{
				//Csq.errCode = GFSErrorCode::invalidPath;
				//std::vector<std::string> tmp;
				//std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, tmp);
				std::cout << "Error 1\n\n";
				return;
			}
			if(pathDeque.size() == 1)
			{
				std::vector<std::string> ans;
				for(int i = 0; i < now->inferior.size(); i++)
					ans.push_back(now->inferior[i]->dirName);
				//Csq.errCode = GFSErrorCode::OK;
				//std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, ans);
				sort(ans.begin(), ans.end());
				for(int i = 0; i < ans.size(); i++)
					std::cout << ans[i] << " ";
				std::cout << "\n\n";
				return;
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
							//Csq.errCode = GFSErrorCode::OK;
							//std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, ans);
							//return Rtn;
							sort(ans.begin(), ans.end());
							for(int i = 0; i < ans.size(); i++)
								std::cout << ans[i] << " ";
							std::cout << "\n\n";
						}
						now = now->inferior[i];
						curPath++;
						continue;
					}	
				}
				std::cout << "Error 2\n\n";
				//std::vector<std::string> tmp;
				//Csq.errCode = invalidPath;
				//std::tuple<GFSError, std::vector<std::string>> Rtn = make_tuple(Csq, tmp);
				return;
			}
		};

int main()
{
	std::string input;
	std::cin >> input;
	dirTree.root->dirName = input;
	int a;
	while(true)
	{
		std::cin >> a >> input;
		std::cout << a << std::endl << input << std::endl;
		if(a == 0)
			std::cout << RPCMkdir(input);
		else
			RPCListFile(input);
	}
	return 0;	
}
