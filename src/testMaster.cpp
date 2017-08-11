#include <iostream>
#include "master.hpp"
#include <service.hpp>

using namespace std;

int main()
{
	LightDS::Service service;
	Master master(service, "/rootDir");
	return 0;
}
