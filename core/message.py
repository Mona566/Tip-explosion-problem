from .global_params import *
from copy import copy

class Message:
    """
    Object to simulate a transaction and its edges in the DAG
    """
    def __init__(self, IssueTime, Parents, Node, Network, Work=0, Index=None, VisibleTime=None, Milestone=False, EnterScheduletime= 0, InScheduletime= 0, MessageInFlag = True):
        self.IssueTime = IssueTime
        self.VisibleTime = VisibleTime
        self.EnterScheduletime = EnterScheduletime
        self.InScheduletime = InScheduletime
        self.MessageInFlag = MessageInFlag
        self.Parents = Parents
        self.Children = []
      
        self.Network = Network
        self.Index = Network.MsgIndex
        self.Milestone = Milestone
        Network.InformedNodes[self.Index] = []
        Network.ScheduledNodes[self.Index] = []
        Network.ConfirmedNodes[self.Index] = []
        self.Work = Work
        self.CWeight = Work
        self.LastCWUpdate = self
        self.Dropped = False
        self.ConfirmedTime = None
        self.LastConfirm = self
        self.Time1 = []
        self.q = []
        self.k = {}
        self.Pastcone = [[] for i in range(100)]
        self.TXConfirmedtime = [[] for i in range(100)]
        self.layer= 0
        self.Parentcone = [[] for i in range(1000)]


        self.Orphan = False
        self.Maxconcone = 0


          
        if Node:
            self.Solid = False
            self.NodeID = Node.NodeID # signature of issuing node
            self.Eligible = False
            self.Confirmed = False
            self.EligibleTime = None
            self.Pastcone[0].append(self) 
            self.TXConfirmedtime[0].append(self.ConfirmedTime)
            Network.MsgIssuer[Network.MsgIndex] = Node.NodeID
        else: # genesis
            self.Solid = True
            self.NodeID = 0 # Genesis is issued by Node 0
            self.Pastcone[0].append(self) 
            self.TXConfirmedtime[0].append(self.ConfirmedTime)
            self.Eligible = True
            self.Confirmed = True
            self.EligibleTime = 0
            self.ConfirmedTime = IssueTime
        Network.MsgIndex += 1


    def mark_confirmed(self, Node = None, Confirtime=None):
        self.Confirmed = True
        self.ConfirmedTime = Confirtime
        self.Network.Nodes[self.NodeID].AllTXconfirmedtime.append(self.ConfirmedTime-self.IssueTime)  
        # self.ConChilup()
        # self.TXConupdate(Node)


        assert Node.NodeID in self.Network.ScheduledNodes[self.Index]
        assert not Node.NodeID in self.Network.ConfirmedNodes[self.Index]
        self.Network.ConfirmedNodes[self.Index].append(Node.NodeID)
        if len(self.Network.ConfirmedNodes[self.Index])==NUM_NODES:
            if self.Index in self.Network.Nodes[self.NodeID].UnconfMsgs:
                self.Network.Nodes[self.NodeID].UnconfMsgs.pop(self.Index)
            self.Network.Nodes[self.NodeID].ConfMsgs[self.Index] = self
        for _,p in self.Parents.items():
            if not p.Confirmed:
                p.mark_confirmed(Node, Confirtime)
    
    def updateCW(self, Node, updateMsg=None, Work=None, UPdateCWtime=None):
        if updateMsg is None:
            assert Work is None
            updateMsg = self
            Work = self.Work
        else:
            assert Work is not None
            self.CWeight += Work
            if self.CWeight >= CONF_WEIGHT:
                self.mark_confirmed(Node,Confirtime=UPdateCWtime)

        self.LastCWUpdate = updateMsg
        for _,p in self.Parents.items():
            if not p.Confirmed and p.LastCWUpdate != updateMsg:
                p.updateCW(Node, updateMsg, Work, UPdateCWtime)


    def TXEcheck(self, Node, Time):

        '''
        Object to do BFS for new incoming tip
        '''
        layer= 0
        Elenumber =[]
        historycone = []
        CheckedTime = []
        Break_flag = False

        while [True for kk in self.Pastcone[layer] if kk not in Elenumber] and self.Pastcone[layer]!=[] and not Break_flag:
            if Elenumber!=[]:          
                for j in self.Pastcone[layer-1]:
                    if j in Elenumber:
                        Elenumber.remove(j) 
            
            for i in self.Pastcone[layer]: 
                if not Break_flag:
                    if i not in Elenumber:
                        Elenumber.append(i)

                    if i in self.Pastcone[layer]:
                        if self.Pastcone[layer].index(i)==0:
                            layer+=1
                    
                    if Time-i.IssueTime<Last_IssuTiime:
                        if len(i.Parents)==0:
                            break
                        else:
                            for _,p in i.Parents.items():
                                self.Pastcone[layer].append(p)
                                # print(self, self.Pastcone[0][0], p, Node.NodeID)
                                            
                        # check whether there are same transactions in this layer
                        Dulcheck = []     
                        for k in self.Pastcone[layer]:
                            if k not in Dulcheck:
                                Dulcheck.append(k)
                        
                        # check whether there are same transactions in this layer
                        self.Pastcone[layer] = Dulcheck
                        for k in range(layer):
                            if len(self.Pastcone[k])!=0:
                                for jj in self.Pastcone[k]:
                                    historycone.append(jj)
                                    

                        for k in self.Pastcone[layer]: 
                            if k in historycone:
                                self.Pastcone[layer].remove(k)
                                              

                        for k in range(layer+1):
                            if len(self.Pastcone[k])!=0:
                                for p in self.Pastcone[k]:
                                    if p.ConfirmedTime!=None:
                                        self.TXConfirmedtime[k].append(p.ConfirmedTime)
                                        if Time-p.ConfirmedTime<=TSC_Th:
                                            Break_flag = True
                                            break



        for i in self.TXConfirmedtime:
            if len(i)!=0:
                for j in i:
                    if j!=None:
                        CheckedTime.append(j)

        if CheckedTime:
            return  max(CheckedTime)      
        else:
            return None                                   
                  

    def copy(self, Node):
        Msg = copy(self)
        parentIDs = [p for p in Msg.Parents]
        parents = {}
        for pID in parentIDs:
            # if we have the parents in the ledger already, include them as parents
            if pID in Node.Ledger:
                parents[pID] = Node.Ledger[pID]
                assert parents[pID].IssueTime<self.IssueTime
            elif pID in Node.SolBuffer:
                parents[pID] = Node.SolBuffer[pID].Data
                assert parents[pID].IssueTime<self.IssueTime
            else:
                parents[pID] = None
        Msg.Parents = parents
        if self.Index == 0:
            Msg.Eligible = True
            Msg.EligibleTime = 0
            Msg.Confirmed = True
            Msg.Solid = True
        else:
            Msg.Eligible = False
            Msg.EligibleTime = None
            Msg.Confirmed = False
            Msg.Solid = False
        return Msg

    def solidify(self, Node, TxNode=None, Time=None):
        solid = True
        for pID, p in self.Parents.items():
            if p is None:
                solid = False
                if pID not in Node.MissingParentIDs:
                    Node.MissingParentIDs[pID] = [self.Index]
                    Node.Network.send_data(Node, TxNode, SolRequest(pID), Time)
                else:
                    if self.Index not in Node.MissingParentIDs[pID]:
                        Node.MissingParentIDs[pID].append(self.Index)
            elif not p.Solid:
                solid = False
                if pID not in Node.MissingParentIDs:
                    Node.MissingParentIDs[pID] = [self.Index]
                else:
                    if self.Index not in Node.MissingParentIDs[pID]:
                        Node.MissingParentIDs[pID].append(self.Index)
        self.Solid = solid
        if self.Solid:
            if self.Index in Node.MissingParentIDs:
                for cID in Node.MissingParentIDs[self.Index]:
                    child = Node.SolBuffer[cID].Data
                    assert self.Index in child.Parents
                    child.Parents[self.Index] = self
                    assert self.IssueTime < child.IssueTime
                    child.solidify(Node)



class SolRequest:
    '''
    Object to request solidification of a transaction
    '''
    def __init__(self, MsgID):
        self.MsgID = MsgID

class PruneRequest:
    """
    Request to prune issued by node "NodeID"
    """
    def __init__(self, NodeID, Forward=False):
        self.NodeID = NodeID
        self.Forward = Forward # flag to forward messages from this node or not
