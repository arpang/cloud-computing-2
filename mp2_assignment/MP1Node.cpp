/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <ctime>
#include <iostream>


/**
 * Todo: (1)When finishing a node send a message that I am leaving the network.
 * (2) Create a handler for that message
 */
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

//long MP1Node::par->getcurrtime() {
//    time_t t = std::time(0);
//    return static_cast<long int> (t);
//}


/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
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
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
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
	/*
	 * This function is partially implemented and may require changes
	 *
	 * Changes made: Insert self's entry into member list
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = TREMOVE;
    initMemberListTable(memberNode);

    MemberListEntry memberListEntry(id, port, memberNode->heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(memberListEntry);
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

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
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
   /*
    * Your code goes here
    */
    memberNode->bFailed = true;
    memberNode->inited = false;
    memberNode->inGroup = false;
    initMemberListTable(memberNode);
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    return 1;
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
    if( !memberNode->inGroup ) {
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
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 * Can receive: JOINREQ or MEMBERSHIPLIST or JOINREP
	 */

    MessageHdr* messageHdrPointer = (MessageHdr*) data;
    if (messageHdrPointer->msgType == JOINREQ) {
        return handleJoinRequest(env, data, size);
    }

    if (messageHdrPointer->msgType == JOINREP) {
        memberNode->inGroup = true;
    }

    if (messageHdrPointer->msgType == MEMBERSHIPLIST || messageHdrPointer->msgType == JOINREP) {
        return handleMembershipUpdate(env, data, size);
    }
    return true;
}

bool MP1Node::handleJoinRequest(void *env, char *data, int size) {

    if (memberNode->bFailed) {
        return false;
    }

    Address newNodeAddress;
    MessageHdr* msg = (MessageHdr*) data;

    int id;
    short port;
    long heartbeat;

    memcpy(&id, (char *)(msg+1), sizeof(int));
    memcpy(&port, (char *)(msg+1) + sizeof(int), sizeof(short));
    memcpy(&heartbeat, (char *)(msg+1) + sizeof(int) + sizeof(short), sizeof(long));
    MemberListEntry memberListEntry(id, port, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(memberListEntry);

    memset(&newNodeAddress, 0, sizeof(Address));
    *(int *)(&newNodeAddress.addr) = id;
    *(short *)(&newNodeAddress.addr[4]) = port;
    log->logNodeAdd(&memberNode->addr, &newNodeAddress);


    size_t msgsize = sizeof(MessageHdr) + memberNode->memberList.size() * (sizeof(MemberListEntry) + 1);

    MessageHdr *reponseMsg = (MessageHdr *) malloc(msgsize * sizeof(char));
    reponseMsg->msgType = JOINREP;

    int copied = 0;
    vector<MemberListEntry>::iterator it;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        MemberListEntry memberListEntry = *it;
        memcpy((char *)(reponseMsg+1) + copied * (sizeof(MemberListEntry)+1), &memberListEntry, sizeof(MemberListEntry));
        copied = copied + 1;
    }


    emulNet->ENsend(&memberNode->addr, &newNodeAddress, (char *)reponseMsg, msgsize);
    free(reponseMsg);
    return true;

    // 1. Get the sender's address
    // 2. Insert the sender to your membership list
    // 3. Log that a new node is added
    // 4. Send the new node your membership list (refer to nodeLoopOps, create message exactly as it has created)

}

bool MP1Node::handleMembershipUpdate(void *env, char *data, int size) {
    MessageHdr* msg = (MessageHdr*) data;



    if (memberNode->bFailed) {
        return false;
    }

//    vector<MemberListEntry> memberList = memberNode->memberList;
    int receivedMemberListSize = (size - sizeof(MessageHdr))/(sizeof(MemberListEntry)+1);
//
//    char static vectorsize[1000];
//    sprintf(vectorsize, "Received memberlist size %d", receivedMemberListSize);
//    log->LOG(&memberNode->addr, vectorsize);


    vector<MemberListEntry> receivedMemberList;
    int copied = 0;
    while(copied < receivedMemberListSize) {
        MemberListEntry memberListEntry;
        memcpy(&memberListEntry, (char*)(msg+1) + copied*(sizeof(MemberListEntry)+1), sizeof(MemberListEntry));
        receivedMemberList.push_back(memberListEntry);
        copied+=1;
    }

//    char static vectorsize[1000];
//    sprintf(vectorsize, "Size of copied vector %d", receivedMemberList.size());
//    log->LOG(&memberNode->addr, vectorsize);


    vector<MemberListEntry>::iterator it;

    for (it = receivedMemberList.begin(); it != receivedMemberList.end(); ++it) {
        int id = it->getid();
        int port = it->getport();
        int heartbeat = it->getheartbeat();
        Address nodeAddress;
        memset(&nodeAddress, 0, sizeof(Address));
        *(int *) (&nodeAddress.addr) = id;
        *(short *) (&nodeAddress.addr[4]) = port;

        if (id == memberNode->memberList.begin()->getid() && port == memberNode->memberList.begin()->getport()) {
            continue;
        }

        vector<MemberListEntry>::iterator current_it = find(id, port);

        if (current_it == memberNode->memberList.end()) {
            MemberListEntry memberListEntry(id, port, heartbeat, par->getcurrtime());
            memberNode->memberList.push_back(memberListEntry);
            log->logNodeAdd(&memberNode->addr, &nodeAddress);
        } else if ((current_it->getheartbeat() < heartbeat)) {
            current_it->settimestamp(par->getcurrtime());
            current_it->setheartbeat(heartbeat);
        }
    }
    return true;
    /**
     *    1. get the membership list from the data
     *    2. Iterate over the nodes:
	 *    - If the node is you: ignore
	 *    - If the node is new: Add it to your membership list, log it
	 *    - If the node is not new and not failed and the received membership list has newer heartbeat: update corresponding membership entry
     */
}

vector<MemberListEntry>::iterator MP1Node::find(int id, short port) {

    vector<MemberListEntry>::iterator it;

    for (it =  memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        if (it->getid() == id && it->getport()==port) {
            return it;
        }
    }
    return it;
}



/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	/*
	 * Your code goes here
	 *
	 * 1. Iterate over the membership list, delete the cleanup nodes
	 *    Log the removed cleanup nodes
	 * 2. Propagate your membership list
	 */

    if (memberNode->pingCounter>0) {
        memberNode->pingCounter--;
        return;
    }
    memberNode->pingCounter = TFAIL;


    memberNode->heartbeat += 1;
    memberNode->memberList.begin()->setheartbeat(memberNode->heartbeat);
    memberNode->memberList.begin()->settimestamp(par->getcurrtime());

    vector<MemberListEntry>::iterator it;

    for (it = memberNode->memberList.begin()+1; it !=memberNode->memberList.end(); ) {
        Address nodeAddress;
        memset(&nodeAddress, 0, sizeof(Address));
        *(int *)(&nodeAddress.addr) = it->getid();
        *(short *)(&nodeAddress.addr[4]) = it->getport();

        if ((par->getcurrtime() - it->gettimestamp()) > TREMOVE) {
            log->logNodeRemove(&memberNode->addr, &nodeAddress);
            it = memberNode->memberList.erase(it);
        } else {
            ++it;
        }
    }


    size_t msgsize = sizeof(MessageHdr) + memberNode->memberList.size()*(sizeof(MemberListEntry) + 1);
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = MEMBERSHIPLIST;
    int copied = 0;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        MemberListEntry memberListEntry = *it;
        memcpy((char *)(msg+1)+ copied * (sizeof(MemberListEntry)+1), &memberListEntry, sizeof(MemberListEntry));
        copied = copied + 1;
    }

//    char static vectorsize[1000];
//    sprintf(vectorsize, "Send memberlist size %d", copied);
//    log->LOG(&memberNode->addr, vectorsize);
    int send = 0;

    for (it = memberNode->memberList.begin()+1; it != memberNode->memberList.end(); ++it) {
//        log->LOG(&memberNode->addr, "Sending message");
        MemberListEntry memberListEntry = *it;
        int id = memberListEntry.getid();
        int port = memberListEntry.getport();

        Address nodeAddress;
        memset(&nodeAddress, 0, sizeof(Address));
        *(int *)(&nodeAddress.addr) = id;
        *(short *)(&nodeAddress.addr[4]) = port;
//        if ((par->getcurrtime() - memberListEntry.gettimestamp()) < memberNode->pingCounter) {
            emulNet->ENsend(&memberNode->addr, &nodeAddress, (char *)msg, msgsize);
            send+=1;
//        }
    }
//    char static vectorsize2[1000];
//    sprintf(vectorsize2, "Message sent to %d", send);
//    log->LOG(&memberNode->addr, vectorsize2);
    free(msg);
    return;
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
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
