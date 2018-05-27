/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for ( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if ( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if ( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	int id = *(int*)(&memberNode->addr.addr);
	short port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode, id, port);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr)) ) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if ( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
    // extract message header
    MessageHdr *message = (MessageHdr *)data;

    switch ( message->msgType ) {
        case JOINREQ:
            handleJOINREQ(env, data, size);
            break;

        case JOINREP:
            handleJOINREP(env, data, size);
            break;

        case GOSSIP:
            handleGOSSIP(env, data, size);
            break;
    }

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    // update my heartbeat and timestamp
    memberNode->heartbeat++;
    memberNode->memberList[0].setheartbeat(memberNode->heartbeat);
    memberNode->memberList[0].settimestamp(par->getcurrtime());

    // remove failed nodes from the membership list
    removeFailedNodes();

    // gossip membership list to select nodes
    gossipMemberList();
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode, int id, short port) {
	memberNode->memberList.clear();
    memberNode->memberList.push_back(
        MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime())
    );
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
    printf(
        "%d.%d.%d.%d:%d \n",
        addr->addr[0],
        addr->addr[1],
        addr->addr[2],
        addr->addr[3],
        *(short*)&addr->addr[4]
    );    
}

/**
 * FUNCTION NAME: serializeMemberList
 * 
 * DESCRIPTION: Serialize the membership list
 */
char * MP1Node::serializeMemberList() {
    char *serializedList;
    vector<MemberListEntry> *memberList = &memberNode->memberList;

    // allocate memory
    size_t size = sizeof(int) + memberList->size() * sizeof(MemberListEntry);
    serializedList = (char *)malloc(size);

    int n = memberList->size(), time = par->getcurrtime();
    for ( int i = 0, j = 0; i < memberList->size(); i++ ) {
        // check if the heartbeat has timed out
        if ( time - (*memberList)[i].gettimestamp() < TFAIL ) {
            memcpy(serializedList + sizeof(int) + j * sizeof(MemberListEntry), &(*memberList)[i], sizeof(MemberListEntry));
            j++;
        } else {
            n--;
        }
    }
    memcpy(serializedList, &n, sizeof(int));

    return serializedList;
}

/**
 * FUNCTION NAME: deserializeMemberList
 * 
 * DESCRIPTION: Deserialize the membership list
 */
vector<MemberListEntry> MP1Node::deserializeMemberList(char *data, int size) {
    vector<MemberListEntry> memberList;

    for ( int i = 0; i < size; i++ ) {
        MemberListEntry *entry = (MemberListEntry *)(data + (i * sizeof(MemberListEntry)));
        memberList.push_back(*entry);
    }

    return memberList;
}

/**
 * FUNCTION NAME: handleJOINREQ
 * 
 * DESCRIPTION: Handle a join request message
 */
void MP1Node::handleJOINREQ(void *env, char *data, int size) {
    char *msg;

    // create JOINREP message header
    MessageHdr msgHeader;
    msgHeader.msgType = JOINREP;

    // get the address and heartbeat of the sender
    Address addr;
    int id;
    short port;
    long heartbeat;
    memcpy(addr.addr, data + sizeof(MessageHdr), sizeof(addr.addr));
    memcpy(&id, addr.addr, sizeof(int));
    memcpy(&port, &addr.addr[4], sizeof(short));
    memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(addr.addr), sizeof(long));

    // get serialized membership list
    char *serializedList = serializeMemberList();

    // allocate memory
    int n;
    memcpy(&n, serializedList, sizeof(int));
    size_t msgsize = sizeof(msgHeader) +
        sizeof(memberNode->addr.addr) +
        sizeof(int) +
        n * sizeof(MemberListEntry);
    msg = (char *)malloc(msgsize);

    // create JOINREP message
    memcpy(msg, &msgHeader, sizeof(msgHeader));
    memcpy(msg + sizeof(msgHeader), addr.addr, sizeof(addr.addr));
    memcpy(msg + sizeof(msgHeader) + sizeof(addr.addr), serializedList, sizeof(int) + n * sizeof(MemberListEntry));

    // send JOINREP message
    emulNet->ENsend(&memberNode->addr, &addr, msg, msgsize);

    // update membership list
    vector<MemberListEntry> list(1, MemberListEntry(id, port, heartbeat, par->getcurrtime()));
    updateMemberList(&list);

    // deallocate memory
    free(msg);
}

/**
 * FUNCTION NAME: handleJOINREP
 * 
 * DESCRIPTION: Handle a join response message
 */
void MP1Node::handleJOINREP(void *env, char *data, int size) {
    // update node status
    memberNode->inGroup = true;

    // deserialize data
    Address addr;
    int n;
    vector<MemberListEntry> memberList;
    memcpy(addr.addr, data + sizeof(MessageHdr), sizeof(addr.addr));
    memcpy(&n, data + sizeof(MessageHdr) + sizeof(addr.addr), sizeof(int));
    memberList = deserializeMemberList(data + sizeof(MessageHdr) + sizeof(addr.addr) + sizeof(int), n);

    updateMemberList(&memberList);
}

/**
 * FUNCTION NAME: handleGOSSIP
 * 
 * DESCRIPTION: Handle a gossip message
 */
void MP1Node::handleGOSSIP(void *env, char *data, int size) {
    // deserialize data
    int n;
    vector<MemberListEntry> memberList;
    memcpy(&n, data + sizeof(MessageHdr), sizeof(int));
    memberList = deserializeMemberList(data + sizeof(MessageHdr) + sizeof(int), n);

    updateMemberList(&memberList);
}

void MP1Node::updateMemberList(vector<MemberListEntry> *inputList) {
    vector<MemberListEntry> *memberList = &memberNode->memberList;
    for ( int i = 0; i < inputList->size(); i++ ) {
        int j;
        for ( j = 0; j < memberList->size(); j++ ) {
            // find an existing entry
            if ( (*inputList)[i].getid() == (*memberList)[j].getid() &&
                (*inputList)[i].getport() == (*memberList)[j].getport() ) {
                // compare heartbeats
                if ((*inputList)[i].getheartbeat() > (*memberList)[j].getheartbeat()) {
                    // update if entry is newer
                    (*memberList)[j].setheartbeat((*inputList)[i].getheartbeat());
                    (*memberList)[j].settimestamp(par->getcurrtime());
                }
                break;
            }
        }
        // if the node isn't in the membership list, add the node
        if ( j == memberList->size() ) {
            // update node status
            memberNode->nnb++;

#ifdef DEBUGLOG
            Address addr = getAddress((*inputList)[i].getid(), (*inputList)[i].getport());
            log->logNodeAdd(&memberNode->addr, &addr);
#endif
            memberNode->memberList.push_back(
                MemberListEntry(
                    (*inputList)[i].getid(),
                    (*inputList)[i].getport(),
                    (*inputList)[i].getheartbeat(),
                    par->getcurrtime()
                )
            );
        }
    }
}

/**
 * FUNCTION NAME: getAddress
 * 
 * DESCRIPTION: Create address object with the given id and port values
 */
Address MP1Node::getAddress(int id, short port) {
    return Address(to_string(id) + ":" + to_string(port));
}

/**
 * FUNCTION NAME: gossipMemberList
 * 
 * DESCRIPTION: Gossip the membership list to select nodes
 */
void MP1Node::gossipMemberList(int n) {
    char *msg;

    // create GOSSIP message header
    MessageHdr msgHeader;
    msgHeader.msgType = GOSSIP;

    // get serialized membership list
    char *serializedList = serializeMemberList();

    // allocate memory
    int size;
    memcpy(&size, serializedList, sizeof(int));
    size_t msgsize = sizeof(msgHeader) +
        sizeof(int) +
        size * sizeof(MemberListEntry);
    msg = (char *)malloc(msgsize);

    // create GOSSIP message
    memcpy(msg, &msgHeader, sizeof(msgHeader));
    memcpy(msg + sizeof(msgHeader), serializedList, sizeof(int) + size * sizeof(MemberListEntry));

    n = min(n, size);
    for ( int i = 0; i < n; i++ ) {
        int idx = rand() % memberNode->memberList.size();

        Address addr = getAddress(
            memberNode->memberList[idx].getid(), memberNode->memberList[idx].getport()
        );
        
        // send GOSSIP
        emulNet->ENsend(&memberNode->addr, &addr, msg, msgsize);
    }

    // deallocate memory
    free(msg);
}

/**
 * FUNCTION NAME: removeFailedNodes
 * 
 * DESCRIPTION: Remove the failed nodes from the membership list
 */
void MP1Node::removeFailedNodes() {
    int time = par->getcurrtime();
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    while ( it != memberNode->memberList.end() ) {
        if ( time - it->gettimestamp() > TREMOVE ) {
            // check if TREMOVE has timed out
#ifdef DEBUGLOG
            Address addr = getAddress(it->getid(), it->getport());
            log->logNodeRemove(&memberNode->addr, &addr);
#endif
            // remove the node from the membership list
            it = memberNode->memberList.erase(it);
        } else {
            it++;
        }
    }
}
