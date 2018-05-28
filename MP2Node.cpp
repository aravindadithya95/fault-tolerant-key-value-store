/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	// check if there has been a change in the ring
	if ( ring.size() != curMemList.size() ) {
		change = true;
	} else {
		for ( int i = 0; i < curMemList.size(); i++ ) {
			if ( curMemList[i].getHashCode() != ring[i].getHashCode() ) {
				change = true;
				break;
			}
		}
	}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if ( !ht->isEmpty() && change ) {
		stabilizationProtocol(&curMemList);
	}

	ring = curMemList;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replicas
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Step 1: Construct the message
	 */
	g_transID++;
	Message primaryMsg(g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
	Message secondaryMsg(g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
	Message tertiaryMsg(g_transID, memberNode->addr, CREATE, key, value, TERTIARY);

	/*
	 * Step 2: Find the replicas of this key
	 */
	vector<Node> replicas = findNodes(key);

	/*
	 * Step 3: Send message to the replicas
	 */
	if ( replicas.size() == 3 ) {
		dispatchMessage(primaryMsg, replicas[0].getAddress());
		dispatchMessage(secondaryMsg, replicas[1].getAddress());
		dispatchMessage(tertiaryMsg, replicas[2].getAddress());
	}

	// keep track of responses to check for quorum
	QuorumEntry entry(CREATE, key, value, par->getcurrtime());
	quorums.emplace(g_transID, entry);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replicas
 */
void MP2Node::clientRead(string key) {
	/*
	 * Step 1: Construct the message
	 */
	g_transID++;
	Message msg(g_transID, memberNode->addr, READ, key);

	/*
	 * Step 2: Find the replicas of this key
	 */
	vector<Node> replicas = findNodes(key);

	/*
	 * Step 3: Send message to the replicas
	 */
	for ( int i = 0; i < replicas.size(); i++ ) {
		dispatchMessage(msg, replicas[i].getAddress());
	}

	// keep track of responses to check for quorum
	QuorumEntry entry(READ, key, par->getcurrtime());
	quorums.emplace(g_transID, entry);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replicas
 */
void MP2Node::clientUpdate(string key, string value) {
	/*
	 * Step 1: Construct the message
	 */
	g_transID++;
	Message primaryMsg(g_transID, memberNode->addr, UPDATE, key, value, PRIMARY);
	Message secondaryMsg(g_transID, memberNode->addr, UPDATE, key, value, SECONDARY);
	Message tertiaryMsg(g_transID, memberNode->addr, UPDATE, key, value, TERTIARY);

	/*
	 * Step 2: Find the replicas of this key
	 */
	vector<Node> replicas = findNodes(key);

	/*
	 * Step 3: Send message to the replicas
	 */
	if ( replicas.size() == 3 ) {
		dispatchMessage(primaryMsg, replicas[0].getAddress());
		dispatchMessage(secondaryMsg, replicas[1].getAddress());
		dispatchMessage(tertiaryMsg, replicas[2].getAddress());
	}

	// keep track of responses to check for quorum
	QuorumEntry entry(UPDATE, key, value, par->getcurrtime());
	quorums.emplace(g_transID, entry);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replicas
 */
void MP2Node::clientDelete(string key) {
	/*
	 * Step 1: Construct the message
	 */
	g_transID++;
	Message msg(g_transID, memberNode->addr, DELETE, key);

	/*
	 * Step 2: Find the replicas of this key
	 */
	vector<Node> replicas = findNodes(key);

	/*
	 * Step 3: Send message to the replicas
	 */
	for ( int i = 0; i < replicas.size(); i++ ) {
		dispatchMessage(msg, replicas[i].getAddress());
	}

	// keep track of responses to check for quorum
	QuorumEntry entry(DELETE, key, par->getcurrtime());
	quorums.emplace(g_transID, entry);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	// Insert key, value, replicaType into the hash table
	return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	// Update key in local hash table and return true or false
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deleteKey(string key) {
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	char * data;
	int size;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		// deserialize the message string
		Message msg(message);

		switch ( msg.type ) {
			case CREATE:
				handleCREATE(&msg);
				break;
			
			case READ:
				handleREAD(&msg);
				break;
			
			case UPDATE:
				handleUPDATE(&msg);
				break;
			
			case DELETE:
				handleDELETE(&msg);
				break;

			case REPLY:
				handleREPLY(&msg);
				break;

			case READREPLY:
				handleREADREPLY(&msg);
				break;
		}
	}

	/*
	 * Ensure all requests timeout sometime, if quorum isn't reached
	 * check QUORUM entries
	 */
	checkQuorumTimeouts();
}

/**
 * FUNCTION NAME: dispatchMessage
 *
 * DESCRIPTION: This function dispatches the message to the corresponding node
 */
void MP2Node::dispatchMessage(Message message, Address *addr) {
	emulNet->ENsend(&memberNode->addr, addr, message.toString());
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(vector<Node> *curMemList) {
	// iterate over the hashtable entries
	map<string, string>::iterator it = ht->hashTable.begin();
	while ( it != ht->hashTable.end() ) {
		string key = it->first, value = it->second;

		// get old and new replicas
		vector<Node> oldReplicas = findNodes(key);
		vector<Node> newReplicas = findReplicas(key, curMemList);

		// if current node WAS the primary replica of the key
		if ( memberNode->addr == oldReplicas[0].nodeAddress ) {
			// check if it still IS the primary replica
			if ( oldReplicas[0].nodeAddress == newReplicas[0].nodeAddress ) {
				// check if tertiary replica has been promoted to secondary (secondary has failed)
				if ( oldReplicas[2].nodeAddress == newReplicas[1].nodeAddress ) {
					// send over pair to new tertiary replica
					Message msg(++g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
					dispatchMessage(msg, newReplicas[2].getAddress());
				}
				else {
					// check if the secondary replica has changed
					if ( !(oldReplicas[1].nodeAddress == newReplicas[1].nodeAddress) ) {
						// send over pair to new secondary replica
						Message msg(++g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
						dispatchMessage(msg, newReplicas[1].getAddress());
					}
					// check if the tertiary replica has changed
					if ( !(oldReplicas[2].nodeAddress == newReplicas[2].nodeAddress) ) {
						// send over pair to new tertiary replica
						Message msg(++g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
						dispatchMessage(msg, newReplicas[2].getAddress());
					}
				}
			}
			// if current node IS secondary replica
			else if ( oldReplicas[0].nodeAddress == newReplicas[1].nodeAddress ) {
				// new primary replica
				Message msg(++g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
				dispatchMessage(msg, newReplicas[0].getAddress());

				// if tertiary replica has changed
				if ( !(oldReplicas[2].nodeAddress == newReplicas[2].nodeAddress) ) {
					// new tertiary replica
					Message msg(++g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
					dispatchMessage(msg, newReplicas[2].getAddress());
				}
			}
			// if current node IS tertiary replica
			else if ( oldReplicas[0].nodeAddress == newReplicas[2].nodeAddress ) {
				// new primary and secondary replicas
				Message primaryMsg(++g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
				Message secondaryMsg(++g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
				dispatchMessage(primaryMsg, newReplicas[0].getAddress());
				dispatchMessage(secondaryMsg, newReplicas[1].getAddress());
			}
			// if current node isn't a replica of this key anymore
			else {
				// new primary replica
					Message msg(++g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
					dispatchMessage(msg, newReplicas[0].getAddress());

				if ( !(newReplicas[1].nodeAddress == oldReplicas[0].nodeAddress) ||
					!(newReplicas[1].nodeAddress == oldReplicas[1].nodeAddress) ||
					!(newReplicas[1].nodeAddress == oldReplicas[2].nodeAddress) ) {
					// new secondary replica
					Message msg(++g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
					dispatchMessage(msg, newReplicas[1].getAddress());
				}

				if ( !(newReplicas[2].nodeAddress == oldReplicas[0].nodeAddress) ||
					!(newReplicas[2].nodeAddress == oldReplicas[1].nodeAddress) ||
					!(newReplicas[2].nodeAddress == oldReplicas[2].nodeAddress) ) {
					// new tertiary replica
					Message msg(++g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
					dispatchMessage(msg, newReplicas[2].getAddress());
				}
			}
		}
		// if current node WAS the secondary replica
		else if ( memberNode->addr == oldReplicas[1].nodeAddress ) {
			// if primary has failed - take over the primary's work to recreate three replicas
			if ( !isPeerAlive(curMemList, &oldReplicas[0].nodeAddress) ) {
				// new primary replica
				Message msg(++g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
				dispatchMessage(msg, newReplicas[0].getAddress());

				// if current node IS secondary replica
				if ( oldReplicas[1].nodeAddress == newReplicas[1].nodeAddress ) {
					// check tertiary replica
					if ( !(oldReplicas[2].nodeAddress == newReplicas[2].nodeAddress) ) {
						// new tertiary replica
						Message msg(++g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
						dispatchMessage(msg, newReplicas[2].getAddress());
					}
				} 
				// if current node is tertiary replica
				else if ( oldReplicas[1].nodeAddress == newReplicas[2].nodeAddress ) {
					// new secondary replica
					Message msg(++g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
					dispatchMessage(msg, newReplicas[1].getAddress());
				}
				// if current node is not a replica anymore
				else {
					// new secondary replica
					Message msg(++g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
					dispatchMessage(msg, newReplicas[1].getAddress());

					// check tertiary replica
					if ( !(oldReplicas[2].nodeAddress == newReplicas[1].nodeAddress) ||
						!(oldReplicas[2].nodeAddress == newReplicas[2].nodeAddress) ) {
						// new tertiary replica
						Message msg(++g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
						dispatchMessage(msg, newReplicas[2].getAddress());
					}
				}
			}
		}
		// if current node WAS the tertiary replica
		else if ( memberNode->addr == oldReplicas[2].nodeAddress ) {
			// if primary and secondary have failed - take over the their work to recreate three replicas
			if ( !isPeerAlive(curMemList, &oldReplicas[0].nodeAddress) &&
				!isPeerAlive(curMemList, &oldReplicas[1].nodeAddress) ) {
				// new primary replica
				Message primaryMsg(++g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
				dispatchMessage(primaryMsg, newReplicas[0].getAddress());

				// new secondary replica
				Message secondaryMsg(++g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
				dispatchMessage(secondaryMsg, newReplicas[1].getAddress());

				// if current node isn't tertiary replica
				if ( !(oldReplicas[2].nodeAddress == newReplicas[2].nodeAddress) ) {
					// new tertiary replica
					Message msg(++g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
					dispatchMessage(msg, newReplicas[2].getAddress());
				}
			}
		}
		it++;
	}
}

/**
 * FUNCTION NAME: handleCREATE
 *
 * DESCRIPTION: Handle an incoming CREATE message
 */
void MP2Node::handleCREATE(Message *msg) {
	g_transID++;

	// enter the pair into the local hash table
	bool success = createKeyValue(msg->key, msg->value, msg->replica);

#ifdef DEBUGLOG
	if ( success ) {
		log->logCreateSuccess(&memberNode->addr, false, g_transID, msg->key, msg->value);
	} else {
		log->logCreateFail(&memberNode->addr, false, g_transID, msg->key, msg->value);
	}
#endif

	// send response back to the coordinator
	Message responseMsg(msg->transID, memberNode->addr, REPLY, success);
	dispatchMessage(responseMsg, &msg->fromAddr);
}

/**
 * FUNCTION NAME: handleREAD
 *
 * DESCRIPTION: Handle an incoming READ message
 */
void MP2Node::handleREAD(Message *msg) {
	g_transID++;

	// find the key-value pair in the local hash table
	string value = readKey(msg->key);

#ifdef DEBUGLOG
	if ( value.size() > 0 ) {
		log->logReadSuccess(&memberNode->addr, false, g_transID, msg->key, value);
	} else {
		log->logReadFail(&memberNode->addr, false, g_transID, msg->key);
	}
#endif

	// send response back to the coordinator
	Message responseMsg(msg->transID, memberNode->addr, value);
	dispatchMessage(responseMsg, &msg->fromAddr);
}

/**
 * FUNCTION NAME: handleUPDATE
 *
 * DESCRIPTION: Handle an incoming READ message
 */
void MP2Node::handleUPDATE(Message *msg) {
	g_transID++;

	// update the pair in the local hash table
	bool success = updateKeyValue(msg->key, msg->value, msg->replica);

#ifdef DEBUGLOG
	if ( success ) {
		log->logUpdateSuccess(&memberNode->addr, false, g_transID, msg->key, msg->value);
	} else {
		log->logUpdateFail(&memberNode->addr, false, g_transID, msg->key, msg->value);
	}
#endif

	// send response back to the coordinator
	Message responseMsg(msg->transID, memberNode->addr, REPLY, success);
	dispatchMessage(responseMsg, &msg->fromAddr);
}

/**
 * FUNCTION NAME: handleDELETE
 *
 * DESCRIPTION: Handle an incoming DELETE message
 */
void MP2Node::handleDELETE(Message *msg) {
	g_transID++;

	// delete the key-value pair in the local hash table
	bool success = deleteKey(msg->key);

#ifdef DEBUGLOG
	if ( success ) {
		log->logDeleteSuccess(&memberNode->addr, false, g_transID, msg->key);
	} else {
		log->logDeleteFail(&memberNode->addr, false, g_transID, msg->key);
	}
#endif

	// send response back to the coordinator
	Message responseMsg(msg->transID, memberNode->addr, REPLY, success);
	dispatchMessage(responseMsg, &msg->fromAddr);
}

/**
 * FUNCTION NAME: handleREPLY
 *
 * DESCRIPTION: Handle an incoming REPLY message
 */
void MP2Node::handleREPLY(Message *msg) {
	// find the quorum entry
	unordered_map<int, QuorumEntry>::iterator entry = quorums.find(msg->transID);
	if ( entry == quorums.end() )	return;

	// check if the ack is positive or negative
	if ( msg->success ) {
		entry->second.positiveAcks++;
	} else {
		entry->second.negativeAcks++;
	}

	// check if quorum is reached (positive or negative) and then delete the entry
	if ( entry->second.positiveAcks == 2 ) {
#ifdef DEBUGLOG
		if ( entry->second.type == CREATE ) {
			log->logCreateSuccess(&memberNode->addr, true, ++g_transID, entry->second.key, entry->second.value);
		} else if ( entry->second.type == UPDATE ) {
			log->logUpdateSuccess(&memberNode->addr, true, ++g_transID, entry->second.key, entry->second.value);
		} else if ( entry->second.type == DELETE ) {
			log->logDeleteSuccess(&memberNode->addr, true, ++g_transID, entry->second.key);
		}
#endif
		quorums.erase(msg->transID);
	} else if ( entry->second.negativeAcks == 2 ) {
#ifdef DEBUGLOG
		if ( entry->second.type == CREATE ) {
			log->logCreateFail(&memberNode->addr, true, ++g_transID, entry->second.key, entry->second.value);
		} else if ( entry->second.type == UPDATE ) {
			log->logUpdateFail(&memberNode->addr, true, ++g_transID, entry->second.key, entry->second.value);
		} else if ( entry->second.type == DELETE ) {
			log->logDeleteFail(&memberNode->addr, true, ++g_transID, entry->second.key);
		}
#endif
		quorums.erase(msg->transID);
	}
}

/**
 * FUNCTION NAME: handleREPLY
 *
 * DESCRIPTION: Handle an incoming READREPLY message
 */
void MP2Node::handleREADREPLY(Message *msg) {
	// find the quorum entry
	unordered_map<int, QuorumEntry>::iterator entry = quorums.find(msg->transID);
	if ( entry == quorums.end() )	return;
	
	// check if the ack is positive or negative
	if ( msg->value.size() > 0 ) {
		entry->second.positiveAcks++;
	} else {
		entry->second.negativeAcks++;
	}

	// check if quorum is reached (positive or negative) and then delete the entry
	if ( entry->second.positiveAcks == 2 ) {
#ifdef DEBUGLOG
		log->logReadSuccess(&memberNode->addr, true, ++g_transID, entry->second.key, msg->value);
#endif
		quorums.erase(msg->transID);
	} else if ( entry->second.negativeAcks == 2 ) {
#ifdef DEBUGLOG
		log->logReadFail(&memberNode->addr, true, ++g_transID, entry->second.key);
#endif
		quorums.erase(msg->transID);
	}
}

/**
 * FUNCTION NAME: checkQuorumTimeouts
 *
 * DESCRIPTION: Check quorum timeouts
 */
void MP2Node::checkQuorumTimeouts() {
	// iterate over hashmap entries
	unordered_map<int, QuorumEntry>::iterator it = quorums.begin();
	while ( it != quorums.end() ) {
		QuorumEntry entry = it->second;
		// check if an entry has timed out
		if ( par->getcurrtime() - entry.time > QUORUM_TIMEOUT ) {
			g_transID++;
#ifdef DEBUGLOG
			if ( entry.type == CREATE ) {
				log->logCreateFail(&memberNode->addr, true, g_transID, entry.key, entry.value);
			} else if ( entry.type == READ ) {
				log->logReadFail(&memberNode->addr, true, g_transID, entry.key);
			} else if ( entry.type == UPDATE ) {
				log->logUpdateFail(&memberNode->addr, true, g_transID, entry.key, entry.value);
			} else if ( entry.type == DELETE ) {
				log->logDeleteFail(&memberNode->addr, true, g_transID, entry.key);
			}
#endif
			it = quorums.erase(it);
		} else {
			it++;
		}
	}
}

/**
 * FUNCTION NAME: findReplicas
 *
 * DESCRIPTION: Find the replicas of the given key in the given member list
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findReplicas(string key, vector<Node> *nodes) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (nodes->size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= nodes->at(0).getHashCode() || pos > nodes->at(nodes->size() - 1).getHashCode()) {
			addr_vec.emplace_back(nodes->at(0));
			addr_vec.emplace_back(nodes->at(1));
			addr_vec.emplace_back(nodes->at(2));
		}
		else {
			// go through the ring until pos <= node
			for ( int i = 1; i < nodes->size(); i++ ){
				Node addr = nodes->at(i);
				if ( pos <= addr.getHashCode() ) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(nodes->at((i + 1) % nodes->size()));
					addr_vec.emplace_back(nodes->at((i + 2) % nodes->size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: isPeerAlive
 *
 * DESCRIPTION: Check if the given peer exists in the given vector
 */
bool MP2Node::isPeerAlive(vector<Node> *nodes, Address *addr) {
	for ( int i = 0; i < nodes->size(); i++ ) {
		if ( nodes->at(i).nodeAddress == *addr ) {
			return true;
		}
	}
	return false;
}
