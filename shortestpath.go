package algorithms

import (
	"encoding/json"
	"fmt"

	//	"strconv"

	"git.cs.upb.de/software/design_backend/topology"
)

// ShortestPath
type ShortestPath struct {
	*Node

	graphStructure [][]float64 //stores the structure of the graph
	visited        []int       //used in graph structure
	distanceUtoV   [][]float64 //used for recursion msg param 3
	discardedlist1 []int

	shortestPathTree  []int //required for dfs tree
	shortestPathTree1 []int //required for dfs tree
	spliTree          []int //split subtree
	splitingtree      []int //split base subtree
	front             int   //required for dfs tree
	rear              int   //required for dfs tree

	parent     map[int]int //required for splitting node
	weight     []int       //required for splitting node
	subTreeMap map[int][]int

	roundCounter    int     //keep info about node's case
	phaseCounter    int     //keep info about node's phase
	loopCounter     int     //keep info about 2 hop distance
	termination     []int   //terminate condition for nodes
	distance4recMsg float64 //used for recursion msg param 2

	RescursionMsgSet    []RecursionMsg
	RescursionMsgSet1   []RecursionMsg
	SplittingMessageSet []SplittingMessage
}

// NewShortestPath node :
func NewShortestPath(node *Node) *ShortestPath {
	n := ShortestPath{Node: node}
	return &n
}

//OnInit function
func (node *ShortestPath) OnInit(args []string) {

	node.loopCounter = 1
	node.roundCounter = 1
	node.phaseCounter = 1
	// chosing 0 is node ID s
	if node.ID == 0 {
		node.distance4recMsg = 0
	} else {
		node.distance4recMsg = -1
	}

	if node.roundCounter == 1 {
		node.SplittingMessageSet = make([]SplittingMessage, 1)
		node.RescursionMsgSet = make([]RecursionMsg, 1)
		node.RescursionMsgSet1 = make([]RecursionMsg, 1)
		node.graphStructure = make([][]float64, node.Handler.GetNumberOfNodes())
		node.visited = make([]int, node.Handler.GetNumberOfNodes())
		node.distanceUtoV = make([][]float64, node.Handler.GetNumberOfNodes())
		node.shortestPathTree = make([]int, node.Handler.GetNumberOfNodes())
		node.shortestPathTree1 = make([]int, node.Handler.GetNumberOfNodes())
		node.termination = make([]int, node.Handler.GetNumberOfNodes())
		node.splitingtree = make([]int, 1)
		node.spliTree = make([]int, 1)
		node.weight = make([]int, node.Handler.GetNumberOfNodes())
		node.parent = make(map[int]int)
		node.front = 0
		node.rear = 0
		node.subTreeMap = make(map[int][]int)
		node.discardedlist1 = make([]int, node.Handler.GetNumberOfNodes())

		//initializing graph structure
		for i := 0; i < len(node.graphStructure); i++ {
			node.termination[i] = 22
			node.visited[i] = 0
			node.shortestPathTree[i] = -1
			node.distanceUtoV[i] = make([]float64, node.Handler.GetNumberOfNodes())
			node.graphStructure[i] = make([]float64, node.Handler.GetNumberOfNodes())
			for j := 0; j < len(node.graphStructure); j++ {
				node.graphStructure[i][j] = 0.0
				node.distanceUtoV[i][j] = 0.0

			}
		}

		//creating local graph
		localNeighbors := node.Handler.GetLocalNeighbors()
		for i := 0; i < len(localNeighbors); i++ {
			eWeight := node.Handler.GetLocalEdgeWeight(localNeighbors[i])
			node.graphStructure[node.ID][localNeighbors[i]] = eWeight
			node.graphStructure[localNeighbors[i]][node.ID] = eWeight

			node.distanceUtoV[node.ID][localNeighbors[i]] = eWeight
			node.distanceUtoV[localNeighbors[i]][node.ID] = eWeight
		}

		//For splittingnode
		node.shortestPathTree[0] = node.ID
		node.parent[node.ID] = -1
		node.visited[node.ID] = 1

		//creating BFS tree
		for node.front <= node.rear {
			//front indicates to the value poped i.e. first value in queue
			temp := node.shortestPathTree[node.front]
			node.front++
			//filling the shortest path array for local neighbours using temp
			// and marking them visited
			for i := 0; i < node.Handler.GetNumberOfNodes(); i++ {
				if node.graphStructure[temp][i] > 0.0 && node.visited[i] == 0 {
					node.rear++
					node.shortestPathTree[node.rear] = i
					node.parent[i] = node.ID
					node.visited[i] = 1

				}
			}
		}

		node.spliTree[0] = -1
		node.splitingtree[0] = -1

		if node.ID == 1 {
			fmt.Println(" Graph structure ", node.graphStructure)
			fmt.Println(" Tree structure ", node.shortestPathTree)
		}
		node.roundCounter++

	}

}

//OnClock function
func (node *ShortestPath) OnClock() {
	var splittingN int
	var discardedlist []int //used for recursion msg param 3
	discardedlist = make([]int, node.Handler.GetNumberOfNodes())

	//process all received messages
	DistMessage := make([]DistanceMessage, len(node.Comm.GetMessages()), len(node.Comm.GetMessages()))

	//***Node phases ****
	switch node.roundCounter {
	case 2:

		//Send  all information about G known so far via incident local edges
		lNeighbors := node.Handler.GetLocalNeighbors()

		for _, current := range lNeighbors {
			msg := GraphStructMsg{node.graphStructure}
			node.Comm.SendMessage(msg, current, false)
		}

		node.loopCounter++
		node.roundCounter++
		break
	case 3:
		// Receive msgs from case2 and send recursion msg to self
		fmt.Println("in case 3 with node ID", node.ID, "------------------------")
		// Receiving msgs
		for _, metamessage := range node.Comm.GetMessages() {

			if metamessage.Type == "GraphStructMsg" {
				//get meta information of message
				sender := metamessage.Src
				receiver := metamessage.Dst

				//create correct message from content of metamessage
				var msg GraphStructMsg
				err := json.Unmarshal(metamessage.Content, &msg)
				if err != nil {
					panic("Could not process message")
				}

				//process custom message
				//remoteGraph will store the localGraph recieved in msg
				remoteGraph := msg.LocalGraph
				var localcal int
				for i := 0; i < len(remoteGraph); i++ {
					for j := 0; j < len(remoteGraph); j++ {
						//localv stores the distance value of reciever node to its neighbour
						localv := node.distanceUtoV[i][j]
						if remoteGraph[i][j] > 0.0 {

							//count the no of leaf nodes
							if sender == i && j != receiver {
								localcal++
							}
							//graph matrix
							//updating the graph structure(adjancy matrix of current receiver node)
							node.graphStructure[i][j] = remoteGraph[i][j]

							//condition1 localv distance value should be 0 and distance of a node to itself is not required
							if (localv == 0 && receiver != j) || (localv > 1 && receiver != j) {
								node.distanceUtoV[i][j] = node.graphStructure[receiver][i] + node.graphStructure[i][j]
							}
						}

					}
					node.weight[sender] = localcal
					//Updating Shortest path tree as per the updated graphStructure
					//checking the remoteGraph elements of sender for non visited node
					if remoteGraph[sender][i] > 0.0 && node.visited[i] == 0 {
						node.rear++
						node.shortestPathTree[node.rear] = i
						node.parent[i] = sender
						node.visited[i] = 1
						node.weight[node.ID]++

					}
				}

			}

		}
		/* Running for 2i hopps */
		if node.loopCounter > 1 {
			node.loopCounter = 0

			//***************** send recursion msg to self ****************************

			recmsg1 := RecursionMsg{node.ID, node.distance4recMsg, discardedlist}
			node.RescursionMsgSet = append(node.RescursionMsgSet, recmsg1)

			node.roundCounter++
		} else {
			node.roundCounter = 2
		}

		if node.ID == 1 {
			fmt.Println(" Tree structure ", node.shortestPathTree)
		}
		break

	case 4:
		var messageId int

		fmt.Println("in case 4 with node ID", node.ID, "------------------------")

		recmsg1 := RecursionMsg{node.ID, 0, discardedlist}
		node.RescursionMsgSet1 = append(node.RescursionMsgSet1, recmsg1)

		for _, msg := range node.RescursionMsgSet {
			if node.distanceUtoV[msg.U][node.ID] > msg.D {

				node.distanceUtoV[msg.U][node.ID] = msg.D

			}
			var wchild int
			var weightofwchild int

			parent1 := node.ID

			//S is subtree of T(u; i) rooted at v without the subtrees rooted at any node of L
			for n := 0; n < len(node.shortestPathTree); n++ {
				for k := 0; k < len(msg.L); k++ {

					if node.parent[n] == parent1 && msg.L[k] != n {

						parent1 = n
						println("------------found new parent--------------------- ")
						break
					}
				}
				if parent1 == n {
					break
				}

			}
			println("exit of breakpoint")

			//maximun s(w)
			for m := 1; m < len(node.shortestPathTree); m++ {

				if node.parent[m] == parent1 {
					if weightofwchild <= node.weight[m] {

						//choosing the node with minimum identifier in case of a tie
						if (wchild > m) && (weightofwchild == node.weight[m]) {
							//do nothing
						} else {
							wchild = m
							weightofwchild = node.weight[wchild]
						}
					}
				}
			}
			//p(u) = V[S] -s(u)
			pOFw := len(node.shortestPathTree) - node.weight[wchild]

			//if p(w) = V[S]/2
			if pOFw < len(node.shortestPathTree)/2 {
				// check for spliting node

				parent1 = wchild
			} else {
				splittingN = wchild //node.parent[wchild]
				//
				node.shortestPathTree1 = node.shortestPathTree

				fmt.Println("splitting node ", splittingN, "--------------------------------for node id", node.ID)
				discardedlist = append(discardedlist, splittingN)
				node.discardedlist1 = discardedlist
				messageId = msg.U
				var splitTree []int //1 hop children of splitting node
				splitTree = make([]int, 1)
				var treeMap map[int][]int //subtrees rooted at children

				treeMap = make(map[int][]int)

				for i := 0; i < len(node.shortestPathTree); i++ {

					if node.parent[node.shortestPathTree[i]] == splittingN && node.parent[node.shortestPathTree[i]] != -1 {
						splitTree = append(splitTree, node.shortestPathTree[i])
					}
				}

				for j := 0; j < len(splitTree); j++ {
					if splitTree[j] != -1 {

						key := splitTree[j]
						var treeArray []int
						treeArray = make([]int, 1)

						for k := 1; k < len(treeArray); k++ {
							treeArray[k] = -1
						}
						treeArray[0] = splitTree[j]

						for m := 0; m < len(treeArray); m++ {

							for n := 0; n < len(node.shortestPathTree); n++ {

								if node.parent[node.shortestPathTree[n]] == treeArray[m] && node.parent[node.shortestPathTree[n]] != -1 && node.shortestPathTree[n] != -1 {

									treeArray = append(treeArray, node.shortestPathTree[n])

								}
							}
							treeMap[key] = treeArray

						}

					}
				}

				node.subTreeMap = treeMap

				for key, element := range node.subTreeMap {
					key = key
					flag1 := 0
					for a := 0; a < len(element); a++ {
						for b := 0; b < len(node.splitingtree); b++ {
							if element[a] == node.splitingtree[b] && element[a] != -1 && node.splitingtree[b] != -1 {
								flag1 = 0
								break
							} else {
								flag1 = 1
							}

						}
						if flag1 == 1 && element[a] != -1 {
							node.splitingtree = append(node.splitingtree, element[a])
						}
					}
				}

				//fill the remaining subtree for recusrion
				var flag int
				flag = 0
				for a := 0; a < len(node.shortestPathTree); a++ {
					for b := 0; b < len(node.splitingtree); b++ {
						if node.shortestPathTree[a] == node.splitingtree[b] && node.shortestPathTree[a] != -1 && node.splitingtree[b] != -1 {
							flag = 0
							break
						} else {
							flag = 1
						}

					}
					if flag == 1 && node.shortestPathTree[a] != -1 {
						node.spliTree = append(node.spliTree, node.shortestPathTree[a])
					}
				}

				break
			}

		}
		if node.ID == 13 {
			fmt.Println(" Tree structure ", node.shortestPathTree)
			fmt.Println(" Tx ", node.subTreeMap)
			fmt.Println(" T-tx ", node.spliTree)

		}

		distancevalue := node.distanceUtoV[messageId][node.ID] + node.distanceUtoV[messageId][splittingN]
		Sptmsg := SplittingMessage{node.ID, distancevalue, int(distancevalue)}
		fmt.Println("sending splitting msg to ", splittingN)

		node.Comm.SendMessage(Sptmsg, splittingN, false)

		distancevalue2 := node.distanceUtoV[messageId][node.ID]
		recmsg2 := RecursionMsg{node.ID, distancevalue2, discardedlist}
		node.RescursionMsgSet = append(node.RescursionMsgSet, recmsg2)

		node.shortestPathTree = node.shortestPathTree[:0]

		node.shortestPathTree = append(node.shortestPathTree, node.spliTree...)

		node.roundCounter++
		break

	case 5:
		// Receiving msgs
		fmt.Println("in case 5 with node ID", node.ID, "------------------------")
		SplitMessage := make([]SplittingMessage, len(node.Comm.GetMessages()), len(node.Comm.GetMessages()))
		for _, metamessage := range node.Comm.GetMessages() {
			if metamessage.Type == "SplittingMessage" {

				var msg SplittingMessage
				err := json.Unmarshal(metamessage.Content, &msg)
				if err != nil {
					panic("Could not process message")
				}

				SplitMessage = append(SplitMessage, msg)
			}
		}

		for _, msg := range SplitMessage {
			for key, element := range node.subTreeMap {
				key = key
				dist := msg.TotalDist
				for p := 0; p < len(element); p++ {
					dist1 := dist + node.distanceUtoV[splittingN][element[p]]

					if element[p] != node.ID {
						node.distanceUtoV[node.ID][element[p]] = dist1
					}
					if msg.TotalDist+node.distanceUtoV[splittingN][element[p]] == node.distanceUtoV[node.ID][element[p]] {
						if node.weight[element[p]] < node.weight[int(node.distanceUtoV[node.ID][element[p]])] {
							node.distanceUtoV[node.ID][element[p]] = msg.TotalDist + node.distanceUtoV[splittingN][element[p]]
						}

					}

					msg1 := RecursionMsg{node.ID, node.distanceUtoV[node.ID][element[p]], node.discardedlist1}

					node.Comm.SendMessage(msg1, element[p], false)
				}
			}
		}

		node.roundCounter++
		break

	case 6:

		Recmessage := make([]RecursionMsg, len(node.Comm.GetMessages()), len(node.Comm.GetMessages()))
		for _, metamessage := range node.Comm.GetMessages() {
			//create correct message from content of metamessage
			//	var Recmessage []RecursionMsg

			if metamessage.Type == "RecursionMsg" {
				var msg RecursionMsg
				err := json.Unmarshal(metamessage.Content, &msg)
				if err != nil {
					panic("Could not process message")
				}

				Recmessage = append(Recmessage, msg)
			}
		}
		for _, msg := range Recmessage {

			//	if msg.D < node.distanceUtoV[node.ID][msg.U] {
			if msg.U != node.ID {
				node.distanceUtoV[node.ID][msg.U] = msg.D
			}
			//	}
			if msg.D == node.distanceUtoV[node.ID][msg.U] {
				if node.weight[msg.U] < node.weight[int(node.distanceUtoV[node.ID][msg.U])] {
					node.distanceUtoV[node.ID][msg.U] = msg.D
				}

			}
			recmsg1 := RecursionMsg{msg.U, node.distanceUtoV[node.ID][msg.U], node.discardedlist1}
			node.RescursionMsgSet1 = append(node.RescursionMsgSet1, recmsg1)

		}
		node.RescursionMsgSet = append(node.RescursionMsgSet, node.RescursionMsgSet1...)
		node.roundCounter++
		fmt.Println("Distance send from case 6 is ", node.graphStructure[node.ID][0], "with node ID", node.ID)
		msg2 := DistanceMessage{node.ID, node.graphStructure[node.ID][0]}
		node.Comm.SendMessage(msg2, 0, false)
		fmt.Println("in case 6 with node ID", node.ID, "------------------------")

		break

	case 7: //Check for termination else repeat round 1-6

		if node.ID == 0 {

			for _, metamessage := range node.Comm.GetMessages() {
				if metamessage.Type == "DistanceMessage" {
					//create correct message from content of metamessage
					var msg DistanceMessage
					err := json.Unmarshal(metamessage.Content, &msg)
					if err != nil {
						panic("Could not process message")
					}

					DistMessage = append(DistMessage, msg)
				}
			}

			//Determining if the all received distance values are unchange by Node ID 0
			for _, msg := range DistMessage {
				fmt.Println("Distance msg sender msg", msg.X, "and  value is", node.distanceUtoV[node.ID][msg.X])
				if msg.TotalDist == node.distanceUtoV[node.ID][msg.X] && node.phaseCounter > 1 {

					//terminate flag decides if case restart of end of execution
					node.termination[msg.X] = 11
				} else {
					node.termination[msg.X] = 22
				}
				//continue with the next phase
				newmsg := Roundcounterbroad{node.termination[msg.X]}
				node.Comm.SendMessage(newmsg, msg.X, false)
			}
			node.phaseCounter++
		}

		fmt.Println("in case 7 with node ID", node.ID, "with round counter", node.roundCounter, "------------------------")
		node.roundCounter++
		break
	case 8:

		for _, metamessage := range node.Comm.GetMessages() {
			switch metamessage.Type {
			case "Roundcounterbroad":

				var msg Roundcounterbroad
				err := json.Unmarshal(metamessage.Content, &msg)
				if err != nil {
					panic("Could not process message")
				}
				node.termination[node.ID] = msg.Cnt
				if msg.Cnt == 22 {

					node.roundCounter = 2

				} else {
					node.roundCounter = 0
				}

				fmt.Println("reveived broadcast at node ID", node.ID, "and node counter", node.roundCounter, "------------------------", "msg cnt", msg.Cnt)

			}
		}
		if node.ID == 0 {
			for j := 0; j < len(node.termination); j++ {
				if node.termination[j] == 22 {
					node.roundCounter = 2
					break
				}

			}
		}

		for x := 0; x < len(node.shortestPathTree1); x++ {
			if node.shortestPathTree1[x] != -1 {
				fmt.Println("Shortest path distance of node id ", node.ID, "to node id ", node.shortestPathTree1[x], "is ", node.distanceUtoV[node.ID][node.shortestPathTree1[x]])
			}
		}
		fmt.Println("in case 8 with node ID", node.ID, "with round counter", node.roundCounter, "------------------------")
		break

	default:
		break

	}

}

//NodeState function
func (node *ShortestPath) NodeState() string {
	return fmt.Sprintf("ID: %v ", node.ID)
}

//************************************** Message Structure*****************************************

//GraphStructMsg contains the Shortest Path
//implements the message.Message interface
type GraphStructMsg struct {
	LocalGraph [][]float64
}

//RecursionMsg contains the Shortest Path
//implements the message.Message interface
type RecursionMsg struct {
	U int     //
	D float64 //candidate value
	L []int   //discarded Path
}

//SplittingMessage contains the Shortest Path
//implements the message.Message interface
type SplittingMessage struct {
	X         int
	TotalDist float64
	Sph       int
}

//DistanceMessage for distance comparison
type DistanceMessage struct {
	X         int
	TotalDist float64
}

// Roundcounterbroad contains a broadcast msg to send all other node round 0 information
type Roundcounterbroad struct {
	Cnt int
}

//GetType return the type of the message as a string
func (msg GraphStructMsg) GetType() string {
	return "GraphStructMsg"
}

//GetType return the type of the message as a string
func (msg RecursionMsg) GetType() string {
	return "RecursionMsg"
}

//GetType return the type of the message as a string
func (msg DistanceMessage) GetType() string {
	return "DistanceMessage"
}

//GetType return the type of the message as a string
func (msg SplittingMessage) GetType() string {
	return "SplittingMessage"
}

//GetType return the type of the message as a string
func (msg Roundcounterbroad) GetType() string {
	return "Roundcounterbroad"
}

//CheckCorrectness function
func (node *ShortestPath) CheckCorrectness(syncNodes []SyncNode, topology topology.Topology) (bool, string) {
	return true, ""
}
