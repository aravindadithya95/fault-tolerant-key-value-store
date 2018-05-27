/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#include<unordered_map>

/**
 * CLASS NAME: QuorumEntry
 * 
 * DESCRIPTION: This class stores information for the coordinator to check for quorums
 */
class QuorumEntry {
public:
	MessageType type;
	string key;
	string value;
	int positiveAcks;
	int negativeAcks;

	// constructor for CREATE and UPDATE quorum entry
	QuorumEntry(MessageType type, string key, string value) {
		this->type = type;
		this->key = key;
		this->value = value;
		positiveAcks = 0;
		negativeAcks = 0;
	}

	// constructor for READ and DELETE quorum entry
	QuorumEntry(MessageType type, string key) {
		this->type = type;
		this->key = key;
		positiveAcks = 0;
		negativeAcks = 0;
	}
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

	// Hash Map for response messages to check for quorums
	unordered_map<int, QuorumEntry> quorums;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// dispatch message to the corresponding node
	void dispatchMessage(Message message, Address *addr);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deleteKey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	void handleCREATE(Message *msg);
	void handleREAD(Message *msg);
	void handleUPDATE(Message *msg);
	void handleDELETE(Message *msg);
	void handleREPLY(Message *msg);
	void handleREADREPLY(Message *msg);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
