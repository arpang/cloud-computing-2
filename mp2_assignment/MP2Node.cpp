/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
#include "common.h"
#include <iostream>
#include <unordered_map>
#include<string>

static int QUORUM = 2;
static int timeout = 20;

/**
 *  char static data[1000];
    string tmp = message.toString();
    sprintf(data, "Sending message: %s to replicas: %d", tmp.c_str(), replicas.size());
    log->LOG(&memberNode->addr, data);
 * Doubts:
 * 1. Stabalization protocol?
 */

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address) {
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


    if (curMemList.size() != ring.size()) {
        change = true;
    } else {
        vector<Node>::iterator it1;
        vector<Node>::iterator it2;
        it2 = curMemList.begin();
        it1 = ring.begin();
        while (it1 != ring.end()) {
            if (strcmp(it1->nodeAddress.addr, it2->nodeAddress.addr) != 0) {
                change = true;
                break;
            }
            ++it1;
            ++it2;
        }
    }

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    if (change) {
        // todo: does this work?
        ring = curMemList;
        if (ht->currentSize() > 0) {
            stabilizationProtocol();
        }
    }
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
    for (i = 0; i < this->memberNode->memberList.size(); i++) {
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
    return ret % RING_SIZE;
}

void MP2Node::dispatchMessages(Message message) {
    vector<Node> replicas = findNodes(message.key);
    vector<Node>::iterator it;

    Node replicaNode = replicas.at(0);
    message.replica = PRIMARY;
    string messageContent = message.toString();
    emulNet->ENsend(&memberNode->addr, replicaNode.getAddress(), (char *) messageContent.c_str(), messageContent.length());

    replicaNode = replicas.at(1);
    message.replica = SECONDARY;
    messageContent = message.toString();
    emulNet->ENsend(&memberNode->addr, replicaNode.getAddress(), (char *) messageContent.c_str(), messageContent.length());

    replicaNode = replicas.at(2);
    message.replica = TERTIARY;
    messageContent = message.toString();
    emulNet->ENsend(&memberNode->addr, replicaNode.getAddress(), (char *) messageContent.c_str(), messageContent.length());
}

transaction MP2Node::initializeTransaction(Message message) {
    transaction t;
    t.message = message.toString();
    t.timestamp = par->getcurrtime();
    t.successCount = 0;
    t.failureCount = 0;
    t.logged = false;
    return t;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    Message message(g_transID, memberNode->addr, CREATE, key, value);
    transactionTracker.emplace(g_transID, initializeTransaction(message));
    g_transID += 1;
    dispatchMessages(message);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
    Message message = Message(g_transID, memberNode->addr, READ, key);
    transactionTracker.emplace(g_transID, initializeTransaction(message));
    g_transID += 1;
    dispatchMessages(message);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
    Message message = Message(g_transID, memberNode->addr, UPDATE, key, value);
    transactionTracker.emplace(g_transID, initializeTransaction(message));
    g_transID += 1;
    dispatchMessages(message);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
    Message message = Message(g_transID, memberNode->addr, DELETE, key);
    transactionTracker.emplace(g_transID, initializeTransaction(message));
    g_transID += 1;
    dispatchMessages(message);
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
    Entry entry = Entry(value, par->getcurrtime(), replica);
    return ht->create(key, entry.convertToString());
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
    string entryString = ht->read(key);
    if (entryString=="") {
        return entryString;
    }
    Entry entry(entryString);
    return entry.value;
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
    Entry entry = Entry(value, par->getcurrtime(), replica);
    return ht->update(key, entry.convertToString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
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
    char *data;
    int size;

    /*
     * Declare your local variables here
     */

    // dequeue all messages and handle them
    while (!memberNode->mp2q.empty()) {
        /*
         * Pop a message from the queue
         */
        data = (char *) memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        string messageString(data, data + size);
        Message message = Message(messageString);

        if (message.type == CREATE) {
            createHandler(message);
        } else if (message.type == UPDATE) {
            updateHandler(message);
        } else if (message.type == DELETE) {
            deleteHandler(message);
        } else if (message.type == READ) {
            readHandler(message);
        } else if (message.type == REPLY) {
            replyHandler(message);
        } else if (message.type == READREPLY) {
            readReplyHandler(message);
        }
    }

    /*
     * This function should also ensure all READ and UPDATE operation
     * get QUORUM replies
     */
    map<int, transaction>::iterator it;
    for(it = transactionTracker.begin(); it!=transactionTracker.end(); ++it) {
        if(it->second.logged) {
            continue;
        }
        Message message(it->second.message);
        if(par->getcurrtime() - it->second.timestamp > timeout) {
            it->second.logged = true;
            logFailureMessage(it->first, message.key, message.value, message.type);
        }
    }
}

void MP2Node::createHandler(Message message) {
    string key = message.key;
    string value = message.value;
    int transID = message.transID;
    Message replyMessage = Message(transID, memberNode->addr, REPLY, true);
    if (createKeyValue(key, value, message.replica)) {
        log->logCreateSuccess(&memberNode->addr, false, transID, key, value);
    } else {
        log->logCreateFail(&memberNode->addr, false, transID, key, value);
        replyMessage.success = false;
    }
    string response = replyMessage.toString();
    emulNet->ENsend(&memberNode->addr, &message.fromAddr, (char *) response.c_str(), response.length());

}

void MP2Node::updateHandler(Message message) {
    string key = message.key;
    string value = message.value;
    int transID = message.transID;
    Message replyMessage = Message(transID, memberNode->addr, REPLY, true);
    if (updateKeyValue(key, value, message.replica)) {
        log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
    } else {
        log->logUpdateFail(&memberNode->addr, false, transID, key, value);
        replyMessage.success = false;
    }
    string response = replyMessage.toString();
    emulNet->ENsend(&memberNode->addr, &message.fromAddr, (char *) response.c_str(), response.length());
}

void MP2Node::deleteHandler(Message message) {
    string key = message.key;
    int transID = message.transID;
    Message replyMessage = Message(transID, memberNode->addr, REPLY, true);
    if (deletekey(key)) {
        log->logDeleteSuccess(&memberNode->addr, false, transID, key);
    } else {
        log->logDeleteFail(&memberNode->addr, false, transID, key);
        replyMessage.success = false;
    }
    string response = replyMessage.toString();
    emulNet->ENsend(&memberNode->addr, &message.fromAddr, (char *) response.c_str(), response.length());
}

void MP2Node::readHandler(Message message) {
    string key = message.key;
    int transID = message.transID;
    string value = readKey(key);
    Message replyMessage = Message(transID, memberNode->addr, value);
    if (value == "") {
        log->logReadFail(&memberNode->addr, false, transID, key);
    } else {
        log->logReadSuccess(&memberNode->addr, false, transID, key, value);
    }
    string response = replyMessage.toString();
    emulNet->ENsend(&memberNode->addr, &message.fromAddr, (char *) response.c_str(), response.length());
}

void MP2Node::logSuccessMessage(int transID, string key, string value, MessageType messageType) {
    if (messageType == CREATE) {
        log->logCreateSuccess(&memberNode->addr, true, transID, key, value);
    } else if (messageType == UPDATE) {
        log->logUpdateSuccess(&memberNode->addr, true, transID, key, value);
    } else if (messageType == DELETE) {
        log->logDeleteSuccess(&memberNode->addr, true, transID, key);
    } else if (messageType == READ) {
        log->logReadSuccess(&memberNode->addr, true, transID, key, value);
    }
}

void MP2Node::logFailureMessage(int transID, string key, string value, MessageType messageType) {
    if (messageType == CREATE) {
        log->logCreateFail(&memberNode->addr, true, transID, key, value);
    } else if (messageType == UPDATE) {
        log->logUpdateFail(&memberNode->addr, true, transID, key, value);
    } else if (messageType == DELETE) {
        log->logDeleteFail(&memberNode->addr, true, transID, key);
    } else if (messageType == READ) {
        log->logReadFail(&memberNode->addr, true, transID, key);
    }
}

void MP2Node::replyHandler(Message replyMessage) {
    int transID = replyMessage.transID;
    map<int, transaction>::iterator it = transactionTracker.find(transID);

    if (it == transactionTracker.end()) {
        return;
    }

    if (it->second.logged) {
        return;
    }

    Message message(it->second.message);

    if (!replyMessage.success) {
        it->second.failureCount +=1;
        if (it->second.failureCount == 2) {
            it->second.logged = true;
            logFailureMessage(transID, message.key, message.value, message.type);
        }
        return;
    }

    it->second.successCount+=1;
    if (it->second.successCount == 2) {
        it->second.logged = true;
        logSuccessMessage(transID, message.key, message.value, message.type);
    }
}


void MP2Node::readReplyHandler(Message replyMessage) {
    int transID = replyMessage.transID;
    string readValue = replyMessage.value;
    map<int, transaction>::iterator it = transactionTracker.find(transID);

    if (it == transactionTracker.end()) {
        return;
    }

    if (it->second.logged) {
        return;
    }

    Message message(it->second.message);

    if (readValue == "") {
        it->second.failureCount +=1;
        if (it->second.failureCount == 2) {
            it->second.logged = true;
            logFailureMessage(transID, message.key, message.value, message.type);
        }
        return;
    }

    it->second.successCount+=1;
    map<string, int>::iterator it2 = it->second.receivedValues.find(readValue);
    if (it2 == it->second.receivedValues.end()) {
        it->second.receivedValues[readValue] = 1;
        return;
    }
    it2->second+=1;
    if (it2->second == 2) {
        it->second.logged = true;
        logSuccessMessage(transID, message.key, readValue, message.type);
    }
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
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        } else {
            // go through the ring until pos <= node
            for (int i = 1; i < ring.size(); i++) {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
                    addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
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
    if (memberNode->bFailed) {
        return false;
    } else {
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
    return q.enqueue((queue<q_elt> *) env, (void *) buff, size);
}

ReplicaType MP2Node::getUpdatedReplicaType(vector<Node> replicas) {
    if(strcmp(replicas.at(0).nodeAddress.addr, memberNode->addr.addr)==0) {
        return PRIMARY;
    }
    if(strcmp(replicas.at(1).nodeAddress.addr, memberNode->addr.addr)==0) {
        return SECONDARY;
    }
    if(strcmp(replicas.at(2).nodeAddress.addr, memberNode->addr.addr)==0) {
        return TERTIARY;
    }
}

int MP2Node::myIndex() {
    int i = 0;
    while (i < ring.size()) {
        if(strcmp(ring.at(i).nodeAddress.addr, memberNode->addr.addr) == 0) {
            break;
        }
        i+=1;
    }
    return i;
}

bool MP2Node::isOneAfterUpdated(int index) {
    if (strcmp(ring.at((index+1) % ring.size()).nodeAddress.addr, hasMyReplicas.at(0).nodeAddress.addr) != 0) {
        hasMyReplicas.at(0) = ring.at((index+1) % ring.size());
        return true;
    }

    return false;
}

bool MP2Node::isTwoAfterUpdated(int index) {
    if (strcmp(ring.at((index+2) % ring.size()).nodeAddress.addr, hasMyReplicas.at(1).nodeAddress.addr) != 0) {
        hasMyReplicas.at(1) = ring.at((index+2) % ring.size());
        return true;
    }
    return false;
}

bool MP2Node::isOneBeforeUpdated(int index) {
    if (strcmp(ring.at((index-1+ring.size()) % ring.size()).nodeAddress.addr, haveReplicasOf.at(0).nodeAddress.addr) != 0) {
        haveReplicasOf.at(0) = ring.at((index-1+ring.size()) % ring.size());
        return true;
    }
    return false;
}

bool MP2Node::isTwoBeforeUpdated(int index) {
    if (strcmp(ring.at((index-2+ring.size()) % ring.size()).nodeAddress.addr, haveReplicasOf.at(1).nodeAddress.addr) != 0) {
        haveReplicasOf.at(1) = ring.at((index-2+ring.size()) % ring.size());
        return true;
    }
    return false;
}

void MP2Node::dispatchMessage(Message message, int index) {
    vector<Node> replicas = findNodes(message.key);
    vector<Node>::iterator it;

    if (index == 0) {
        message.replica = PRIMARY;
    } else if (index == 1) {
        message.replica = SECONDARY;
    } else if (index == 2) {
        message.replica = TERTIARY;
    }

    Node replicaNode = replicas.at(index);
    string messageContent = message.toString();
    emulNet->ENsend(&memberNode->addr, replicaNode.getAddress(), (char *) messageContent.c_str(), messageContent.length());
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
void MP2Node::stabilizationProtocol() {
    /**
     *  1. Do internal shuffling.
     *  2. If nodes containing replicas have changed (including the case when I am a new node):
     *      (a) If secondary replica has changed: send entire of my primary content to it
     *      (b) If tertiary replica has changed: send entire of my primary content to it
     *  3. If nodes whose replicas I have, have changed:
     *      (a) If one before has changed: send entire of secondary content to it
     *      (b) If two before has changed: send entire of tertiary content to it
     *  4. Update hasReplicas and haveReplicasOf
     */

    if (ring.size() < 3) {
        return;
    }
    map<string, string>::iterator it;

    for (it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it) {
        vector<Node> replicas = findNodes(it->first);
        Entry entry(it->second);
        ReplicaType newReplicaType = getUpdatedReplicaType(replicas);

        if (entry.replica != newReplicaType) {
            entry.replica = newReplicaType;
            it->second = entry.convertToString();
        }
    }

    int index = myIndex();
    if (index == ring.size()) {
        return;
    }

    bool oneAfterUpdated;
    bool twoAfterUpdated;
    bool oneBeforeUpdated;
    bool twoBeforeUpdated;

    if (hasMyReplicas.size() == 0) {
        hasMyReplicas.emplace_back(ring.at((index+1) % ring.size()));
        hasMyReplicas.emplace_back(ring.at((index+2) % ring.size()));
        oneAfterUpdated = true;
        twoAfterUpdated = true;
    } else {
        oneAfterUpdated = isOneAfterUpdated(index);
        twoAfterUpdated = isTwoAfterUpdated(index);
    }

    if (haveReplicasOf.size() == 0) {
        haveReplicasOf.emplace_back(ring.at((index-1+ring.size()) % ring.size()));
        haveReplicasOf.emplace_back(ring.at((index-2+ring.size()) % ring.size()));
        oneBeforeUpdated = true;
        twoBeforeUpdated = true;
    } else {
        oneBeforeUpdated = isOneBeforeUpdated(index);
        twoBeforeUpdated = isTwoBeforeUpdated(index);
    }

    for (it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it) {
        string key = it->first;
        Entry entry(it->second);
        Message message(0, memberNode->addr, CREATE, key, entry.value);

        if (entry.replica == PRIMARY) {
            if (oneAfterUpdated) {
                dispatchMessage(message, 1);
            }

            if (twoAfterUpdated) {
                dispatchMessage(message, 2);
            }
        }

        if (entry.replica == SECONDARY && oneBeforeUpdated) {
            dispatchMessage(message, 0);
        }

        if (entry.replica == TERTIARY && twoBeforeUpdated) {
            dispatchMessage(message, 0);
        }

    }
}
