/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/**
 * Message Types
 */
enum MsgTypes {
    JOINREQ,
    JOINREP,
    GOSSIP
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
} MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode, int id, short port);
	void printAddress(Address *addr);
	virtual ~MP1Node();

	char * serializeMemberList();
	vector<MemberListEntry> deserializeMemberList(char *data, int size);
	void handleJOINREQ(void *env, char *data, int size);
	void handleJOINREP(void *env, char *data, int size);
	void handleGOSSIP(void *env, char *data, int size);
	void updateMemberList(vector<MemberListEntry> *inputList);
	Address getAddress(int id, short port);
	void gossipMemberList(int n=2);
	void sendMemberList(MsgTypes msgType, Address *addr);
	void removeFailedNodes();
};

#endif /* _MP1NODE_H_ */
