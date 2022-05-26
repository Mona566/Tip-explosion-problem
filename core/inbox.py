from .global_params import *
from . import message as msg
import numpy as np

class Inbox:
    """
    Object for holding packets in different channels corresponding to different nodes
    """
    def __init__(self, Node):
        self.Node = Node
        self.AllPackets = [] # Inbox_m
        self.AllReadyPackets = []
        self.Packets = [[] for NodeID in range(NUM_NODES)] # Inbox_m(i)
        self.ReadyPackets = [[] for NodeID in range(NUM_NODES)] # Inbox_m(i)
        self.Work = np.zeros(NUM_NODES)
        self.MsgIDs = []
        self.RRNodeID = np.random.randint(NUM_NODES) # start at a random node
        self.Deficit = np.zeros(NUM_NODES)
        self.Scheduled = []
        self.Avg = 0
        self.RequestedMsgIDs = []
        self.DroppedPackets = {}
        self.DelayedMess = []
        self.DelayedMessTime = []
        self.DelayedTxNodeID =[[] for NodeID in range(NUM_NODES)]
        self.Malicioustxtime = []
        self.MaliciousDelayedTxNodeID =[[] for NodeID in range(NUM_NODES)]
        self.a= []
        self.b= []


    def update_ready(self):
        """
        Needs to be updated to only check when needed
        """
        for pkt in self.AllPackets:
            if pkt not in self.AllReadyPackets and self.is_ready(pkt.Data):
                self.AllReadyPackets.append(pkt)
                self.ReadyPackets[pkt.Data.NodeID].append(pkt)
        
    def is_ready(self, Msg):
        for pID,p in Msg.Parents.items():
            if not p.Eligible and not p.Confirmed:
                if pID in self.DroppedPackets:
                    Packet = self.DroppedPackets.pop(pID)
                    self.Node.enqueue(Packet)
                return False
        return True
    
    def add_packet(self, Packet):
        Msg = Packet.Data
        assert isinstance(Msg, msg.Message)
        NodeID = Msg.NodeID
        if Msg.Index in self.RequestedMsgIDs:
            if self.Packets[NodeID]:
                Packet.EndTime = self.Packets[NodeID][0].EndTime # move packet to the front of the queue
            self.RequestedMsgIDs = [msgID for msgID in self.RequestedMsgIDs if msgID != Msg.Index]
            self.Packets[NodeID].insert(0,Packet)
        else:
            self.Packets[NodeID].append(Packet)
        self.AllPackets.append(Packet)
        self.MsgIDs.append(Msg.Index)
        # check parents are eligible
        if self.is_ready(Msg):
            self.AllReadyPackets.append(Packet)
            self.ReadyPackets[NodeID].append(Packet)
        self.Work[NodeID] += Packet.Data.Work
       
    def remove_packet(self, Packet):
        """
        Remove from Inbox and filtered inbox etc
        """
        if self.MsgIDs:
            if Packet in self.AllPackets:
                self.AllPackets.remove(Packet)
                if Packet in self.AllReadyPackets:
                    self.AllReadyPackets.remove(Packet)
                    self.ReadyPackets[Packet.Data.NodeID].remove(Packet)
                self.Packets[Packet.Data.NodeID].remove(Packet)
                #if Packet.Data.Index in self.MsgIDs:
                self.MsgIDs.remove(Packet.Data.Index)  
                self.Work[Packet.Data.NodeID] -= Packet.Data.Work

    def drop_packet(self, Packet):
        self.remove_packet(Packet)
        self.DroppedPackets[Packet.Data.Index] = Packet
    
    def drr_lds_schedule(self, Time):
        if self.Scheduled:
            return self.Scheduled.pop(0)
        
        Packets = [self.AllPackets]
        while Packets[0] and not self.Scheduled:
            if self.Deficit[self.RRNodeID]<MAX_WORK:
                self.Deficit[self.RRNodeID] += QUANTUM[self.RRNodeID]
            i = 0
            while self.Packets[self.RRNodeID] and i<len(self.Packets[self.RRNodeID]):
                Packet = self.Packets[self.RRNodeID][i]
                if Packet not in Packets[0]:
                    i += 1
                    continue
                Work = Packet.Data.Work
                if self.Deficit[self.RRNodeID]>=Work and Packet.EndTime<=Time:
                    self.Deficit[self.RRNodeID] -= Work
                    # remove the message from all inboxes
                    self.remove_packet(Packet)
                    self.Scheduled.append(Packet)
                else:
                    i += 1
                    continue
            self.RRNodeID = (self.RRNodeID+1)%NUM_NODES
        if self.Scheduled:
            return self.Scheduled.pop(0)

    def drr_ready_schedule(self, Time, Systemtime):
        '''
        Deficit round robin is a scheduling algorithm that iterates through a list of queues and tries to
        send as many messages as possible from each queue.

        1. algorithm selects the next queue
        2. it adds 'q' deficit to the node, that is proportional to access mana. the total deficit that a node can accumulate is limited 
        3. if the queue is not empty and there is enough deficit accumulated, the scheduler takes the next ready message from the queue and schedules it.
            some deficit is subtracted from the node, according to a work function
        4. go to 1
        '''

        if self.Scheduled:
            return self.Scheduled.pop(0)
        
        while self.AllReadyPackets and not self.Scheduled:
            if self.Deficit[self.RRNodeID]<MAX_WORK:
                self.Deficit[self.RRNodeID] += QUANTUM[self.RRNodeID]

            for Packets in self.ReadyPackets:
                if Packets!=[]:
                    for Packet in Packets:
                        Packet.Data.InScheduletime = Systemtime

            abc = self.Node.NodeID

            i = 0
            while self.ReadyPackets[self.RRNodeID] and i<len(self.ReadyPackets[self.RRNodeID]):
                Packet = self.ReadyPackets[self.RRNodeID][i]
                Work = Packet.Data.Work   
                Packet.Data.InScheduletime = Systemtime     
                Insidetime= Packet.Data.InScheduletime - Packet.Data.EnterScheduletime   



                if self.Deficit[self.RRNodeID]>=Work and Packet.EndTime<=Time:
                    self.Deficit[self.RRNodeID] -= Work
                    # remove the message from all inboxes
                    self.remove_packet(Packet)
                    self.Scheduled.append(Packet)
                else:
                    i += 1
                    continue

            self.RRNodeID = (self.RRNodeID+1)%NUM_NODES

        if self.Scheduled:
            return self.Scheduled.pop(0) 
                    
    def fifo_schedule(self, Time):
        if self.AllPackets:
            Packet = self.AllPackets[0]
            # remove the message from all inboxes
            self.remove_packet(Packet)
            return Packet