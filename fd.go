package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Master struct {
	totalWorkers      int
	reports           map[int][]int // key: worker value: list of rounds
	confirmFailures   map[int][]int // key: worker value: list of rounds
	falsePositivesMap map[int][]int // key: worker value: list of rounds
	falsePositives    int
	truePositives     int
	falseNegatives    int
	trueNegatives     int
	failureThreshold  int
	messagesProcessed int
}

func (master *Master) initialize(numOfWorkers, failureThreshold int) {
	master.totalWorkers = numOfWorkers
	master.failureThreshold = failureThreshold
	master.messagesProcessed = 0
	master.reports = make(map[int][]int)
	master.confirmFailures = make(map[int][]int)
	master.falsePositivesMap = make(map[int][]int)
}

func (master *Master) recvFailureReports(network *Network, workers []Worker, ROUND int) {
	reports := network.recv(master.totalWorkers, -1)
	reported := make([]int, 0)

	for _, report := range reports {
		failed, _ := strconv.Atoi(report["Failed"])
		reporter, _ := strconv.Atoi(report["Reporter"])
		round, _ := strconv.Atoi(report["Round"])

		var cycles int = (round - 1) / (master.totalWorkers - 1)

		if (failed+round+cycles)%master.totalWorkers != reporter {
			fmt.Println("Failure reported by wrong worker", "Reporter:", reporter, "Failed:", failed, "Round:", round, "Actual round", ROUND)
		}
		// fmt.Println("MASTER:Failure of", failed, "reported by", reporter, "for round", round, "in round", ROUND)
		master.reports[failed] = append(master.reports[failed], ROUND)

		isFailed := master.checkConsecutiveReports(failed)

		if isFailed {
			reported = append(reported, failed)
			if workers[failed].killed == false {
				master.falsePositives++
				master.falsePositivesMap[failed] = append(master.falsePositivesMap[failed], round)
			} else {
				master.truePositives++
			}
			// fmt.Println("Failed", master.reports[failed])
		} else {
			// fmt.Println("not failed", master.reports[failed])
		}
		master.messagesProcessed++
	}

	for i := 0; i < master.totalWorkers; i++ {
		negative := true
		for j := 0; j < len(reported); j++ {
			if i == reported[j] {
				negative = false
				break
			}
		}

		if negative {
			if workers[i].killed == true {
				master.falseNegatives++
			} else {
				master.trueNegatives++
			}
		}
	}
}

func (master *Master) recvHeartbeats(network *Network, workers []Worker, ROUND int) {

	reported := make([]int, 0)
	for i := 0; i < master.totalWorkers; i++ {
		messages := network.recv(master.totalWorkers, i)

		if len(messages) == 0 {
			master.reports[i] = append(master.reports[i], ROUND)

			isFailed := master.checkConsecutiveReports(i)

			if isFailed {
				reported = append(reported, i)
				if workers[i].killed == false {
					master.falsePositives++
					master.falsePositivesMap[i] = append(master.falsePositivesMap[i], ROUND)
				} else {
					master.truePositives++
				}
				// fmt.Println("Failed", master.reports[i])
			} else {
				// fmt.Println("not failed", master.reports[i])
			}
		} else if len(messages) == 1 {
			if messages[0]["Msg"] != "Heartbeat" {
				fmt.Println("Expected Heartbeat. Got something else")
			} else if round, _ := strconv.Atoi(messages[0]["Round"]); round != ROUND {
				fmt.Println("Unexpected round value in heartbeat")
			}
		} else {
			fmt.Println("Master got more than 1 message while receiving heartbeats")
		}

		master.messagesProcessed += len(messages)
	}

	for i := 0; i < master.totalWorkers; i++ {
		negative := true
		for j := 0; j < len(reported); j++ {
			if i == reported[j] {
				negative = false
				break
			}
		}

		if negative {
			if workers[i].killed == true {
				master.falseNegatives++
			} else {
				master.trueNegatives++
			}
		}
	}
}

func (master *Master) checkConsecutiveReports(failedWorker int) bool {
	if len(master.reports[failedWorker]) < master.failureThreshold {
		return false
	}

	for i := 0; i < master.failureThreshold-1; i++ {
		if master.reports[failedWorker][len(master.reports[failedWorker])-i-1] > master.reports[failedWorker][len(master.reports[failedWorker])-i-2]+master.totalWorkers-1 {
			return false
		}
	}

	master.confirmFailures[failedWorker] = append(master.confirmFailures[failedWorker], master.reports[failedWorker][len(master.reports[failedWorker])-1])
	return true
}

func (master *Master) reportTimeToAllDetection() int {
	maxRound := -1
	for i := 0; i < master.totalWorkers; i++ {
		if len(master.confirmFailures[i]) > 0 && maxRound < master.confirmFailures[i][0] {
			maxRound = master.confirmFailures[i][0]
		}
	}
	// fmt.Println("All failures detected by round", maxRound)

	return maxRound
}

func (master *Master) reportConfirmedFailures() int {
	failures := 0

	for i := 0; i < master.totalWorkers; i++ {
		if len(master.confirmFailures[i]) > 0 {
			failures++
		}
	}
	return failures
}

type Worker struct {
	index        int
	sendIndex    int
	recvIndex    int
	totalWorkers int
	killed       bool
}

func (worker *Worker) initialize(index, numOfWorkers int) {
	worker.index = index
	worker.killed = false
	worker.totalWorkers = numOfWorkers
	worker.sendIndex = (index + 1) % worker.totalWorkers
	worker.recvIndex = ((index-1)%worker.totalWorkers + worker.totalWorkers) % worker.totalWorkers
}

func (worker *Worker) sendHeartbeatToWorker(network *Network, ROUND int) {

	if worker.killed {
		return
	}

	heartbeat := make(map[string]string)
	heartbeat["Msg"] = "Heartbeat"
	heartbeat["Round"] = fmt.Sprintf("%d", ROUND)
	heartbeat["Index"] = fmt.Sprintf("%d", worker.index)
	network.send(worker.sendIndex, worker.index, heartbeat)

	// fmt.Println(worker.index, " is sending heartbeat to ", worker.sendIndex)
	// channels[worker.index][worker.sendIndex] <- "Heartbeat"
	// fmt.Println("hb sent")
}

func (worker *Worker) sendHeartbeatToMaster(network *Network, ROUND int) {

	if worker.killed {
		return
	}

	heartbeat := make(map[string]string)
	heartbeat["Msg"] = "Heartbeat"
	heartbeat["Round"] = fmt.Sprintf("%d", ROUND)
	heartbeat["Index"] = fmt.Sprintf("%d", worker.index)
	network.send(worker.totalWorkers, worker.index, heartbeat)

	// fmt.Println(worker.index, " is sending heartbeat to ", worker.sendIndex)
	// channels[worker.index][worker.sendIndex] <- "Heartbeat"
	// fmt.Println("hb sent")
}

func (worker *Worker) recvHeartbeat(network *Network, ROUND int) {

	if worker.killed {
		return
	}

	messages := network.recv(worker.index, worker.recvIndex)

	if len(messages) == 0 {
		worker.reportFailure(network, ROUND)
	} else if len(messages) == 1 {
		if messages[0]["Msg"] != "Heartbeat" {
			fmt.Println("Expected Heartbeat. Got something else")
		} else if round, _ := strconv.Atoi(messages[0]["Round"]); round != ROUND {
			// } else if round, _ := strconv.Atoi(messages[0]["Round"]); round != ROUND-1 {
			fmt.Println("Unexpected round value in heartbeat")
		} else if index, _ := strconv.Atoi(messages[0]["Index"]); index != worker.recvIndex {
			fmt.Println("Got unexpected heartbeat")
		}
	} else {
		fmt.Println("Got more than 1 message while receiving heartbeats")
	}
}

func (worker *Worker) reportFailure(network *Network, ROUND int) {
	failureReport := make(map[string]string)
	failureReport["Failed"] = fmt.Sprint(worker.recvIndex)
	failureReport["Reporter"] = fmt.Sprint(worker.index)
	failureReport["Round"] = fmt.Sprint(ROUND)
	network.send(network.totalWorkers, worker.index, failureReport)
	// fmt.Println("WORKER:Failure of", worker.recvIndex, "reported by", worker.index, "in round", ROUND)
}

func (worker *Worker) updateHeartbeatIndices() {
	worker.sendIndex = (worker.sendIndex + 1) % worker.totalWorkers
	worker.recvIndex = ((worker.recvIndex-1)%worker.totalWorkers + worker.totalWorkers) % worker.totalWorkers
}

type Network struct {
	bufferSize   int
	totalWorkers int
	load         int
	links        map[int]map[int]chan map[string]string
	thresholds   *map[int]map[int]float64
}

func (network *Network) initialize(numOfWorkers, bufferSize int, thresholds *map[int]map[int]float64) {
	network.bufferSize = bufferSize
	network.totalWorkers = numOfWorkers
	network.load = 0
	network.thresholds = thresholds
	network.links = make(map[int]map[int]chan map[string]string)
	for i := 0; i <= numOfWorkers; i++ {
		network.links[i] = make(map[int]chan map[string]string)
		for j := 0; j <= numOfWorkers; j++ {
			network.links[i][j] = make(chan map[string]string, bufferSize)
		}
	}
}

func (network *Network) clearMasterChannels() {
	network.links[network.totalWorkers] = make(map[int]chan map[string]string)
	for j := 0; j <= network.totalWorkers; j++ {
		network.links[network.totalWorkers][j] = make(chan map[string]string, network.bufferSize)
	}
}

func (network *Network) clearWorkerChannels() {
	for i := 0; i < network.totalWorkers; i++ {
		network.links[i] = make(map[int]chan map[string]string)
		for j := 0; j <= network.totalWorkers; j++ {
			network.links[i][j] = make(chan map[string]string, network.bufferSize)
		}
	}
}

func (network *Network) clearAllChannels() {
	for i := 0; i <= network.totalWorkers; i++ {
		network.links[i] = make(map[int]chan map[string]string)
		for j := 0; j <= network.totalWorkers; j++ {
			network.links[i][j] = make(chan map[string]string, network.bufferSize)
		}
	}
}

func (network *Network) recv(to, from int) []map[string]string {
	messages := make([]map[string]string, 0)

	// recv from everyone
	if from == -1 {
		for i := 0; i < network.totalWorkers; i++ {
			numOfMessages := len(network.links[to][i])
			if numOfMessages > 0 {
				// fmt.Println("Got something")
				for j := 0; j < numOfMessages; j++ {
					messages = append(messages, <-network.links[to][i])
				}
			}
		}
	} else {
		numOfMessages := len(network.links[to][from])
		if numOfMessages > 0 {
			for j := 0; j < numOfMessages; j++ {
				messages = append(messages, <-network.links[to][from])
			}
		}
	}
	return messages
}

func (network *Network) send(to, from int, message map[string]string) {
	if len(network.links[to][from]) < network.bufferSize {
		if rand.NormFloat64() <= (*network.thresholds)[to][from] {
			network.links[to][from] <- message
		}
	} else {
		fmt.Println("Channel is full while sending message to ", to, "from", from)
	}
	network.load++
}

func runDistributedSimulation(config Scenario, channel chan map[string]int) {
	channel <- distributedScheme(config, 1)
}

func runCentralSimulation(config Scenario, channel chan map[string]int) {
	channel <- centralScheme(config, 1)
}

type Scenario struct {
	totalWorkers      int
	rounds            int
	workerState       map[int]map[int]int
	networkThresholds map[int]map[int]float64
	failureThreshold  int
	failures          int
}

func distributedScheme(scenario Scenario, bufferSize int) map[string]int {
	// Initializing
	ROUND := 1
	numOfWorkers := scenario.totalWorkers
	master := Master{}
	workers := make([]Worker, numOfWorkers)
	network := Network{}
	results := make(map[string]int)

	master.initialize(numOfWorkers, scenario.failureThreshold)
	network.initialize(numOfWorkers, bufferSize, &scenario.networkThresholds)

	for i := 0; i < numOfWorkers; i++ {
		workers[i].initialize(i, numOfWorkers)
	}

	// Main processing
	for i := 0; i < scenario.rounds; i++ {

		// Master receives previous round's failure reports
		master.recvFailureReports(&network, workers, ROUND)
		network.clearMasterChannels()

		// Change worker alive status according to scenario for this round
		if scenario.workerState[ROUND] != nil {
			for k, v := range scenario.workerState[ROUND] {
				if v == 1 {
					workers[k].killed = false
				} else if v == -1 {
					workers[k].killed = true
				}
			}
		}

		// Workers send heartbeats
		for _, v := range workers {
			v.sendHeartbeatToWorker(&network, ROUND)
		}

		// Workers receive and process previous round's heartbeats
		for _, v := range workers {
			v.recvHeartbeat(&network, ROUND)
		}
		network.clearWorkerChannels()

		// Get ready for this round. Clear network channels and update worker state
		for i := 0; i < numOfWorkers; i++ {
			workers[i].updateHeartbeatIndices()
		}

		// Skip round where workers send and recv hb from themselves
		if ROUND%(numOfWorkers-1) == 0 {
			for i := 0; i < numOfWorkers; i++ {
				workers[i].updateHeartbeatIndices()
			}
		}
		ROUND++
	}

	// Finishing
	// fmt.Printf("Total Time is: %d\n", ROUND-1)
	results["System"] = 1
	results["Time To All Detection"] = master.reportTimeToAllDetection()
	results["False Positives"] = master.falsePositives
	results["True Positives"] = master.truePositives
	results["False Negatives"] = master.falseNegatives
	results["True Negatives"] = master.trueNegatives
	results["Master Load"] = master.messagesProcessed
	results["Confirmed Failures"] = master.reportConfirmedFailures()
	results["Network Load"] = network.load
	results["Failures"] = scenario.failures
	return results
}

func centralScheme(scenario Scenario, bufferSize int) map[string]int {
	// Initializing
	ROUND := 1
	numOfWorkers := scenario.totalWorkers
	master := Master{}
	workers := make([]Worker, numOfWorkers)
	network := Network{}
	results := make(map[string]int)

	master.initialize(numOfWorkers, scenario.failureThreshold)
	network.initialize(numOfWorkers, bufferSize, &scenario.networkThresholds)

	for i := 0; i < numOfWorkers; i++ {
		workers[i].initialize(i, numOfWorkers)
	}

	// Main processing
	for i := 0; i < scenario.rounds; i++ {
		// Change worker alive status according to scenario for this round
		if scenario.workerState[ROUND] != nil {
			for k, v := range scenario.workerState[ROUND] {
				if v == 1 {
					workers[k].killed = false
				} else if v == -1 {
					workers[k].killed = true
				}
			}
		}

		// Workers send heartbeats
		for _, v := range workers {
			v.sendHeartbeatToMaster(&network, ROUND)
		}

		// Master receives previous round's failure reports
		master.recvHeartbeats(&network, workers, ROUND)
		network.clearMasterChannels()

		ROUND++
	}

	// Finishing
	// fmt.Printf("Total Time is: %d\n", ROUND-1)
	results["System"] = 0
	results["Time To All Detection"] = master.reportTimeToAllDetection()
	results["False Positives"] = master.falsePositives
	results["True Positives"] = master.truePositives
	results["False Negatives"] = master.falseNegatives
	results["True Negatives"] = master.trueNegatives
	results["Master Load"] = master.messagesProcessed
	results["Confirmed Failures"] = master.reportConfirmedFailures()
	results["Network Load"] = network.load
	results["Failures"] = scenario.failures
	return results
}

func main() {
	iterations := 10
	workers := 10
	rounds := 100
	failureThreshold := 3
	totalFailedWorkers := 0
	totalTime := 0
	totalFalsePositives := 0
	totalTruePositives := 0
	totalFalseNegatives := 0
	totalTrueNegatives := 0
	totalMasterLoad := 0
	totalConfirmedFailures := 0
	totalNetworkLoad := 0

	totalCentralTime := 0
	totalCentralFalsePositives := 0
	totalCentralTruePositives := 0
	totalCentralFalseNegatives := 0
	totalCentralTrueNegatives := 0
	totalCentralMasterLoad := 0
	totalCentralConfirmedFailures := 0
	totalCentralNetworkLoad := 0
	channel := make(chan map[string]int, 100)
	rand.Seed(time.Now().UnixNano())

	file, _ := os.Create(fmt.Sprintf("results_%d.csv", time.Now().UnixNano()))
	defer file.Close()
	w := csv.NewWriter(file)
	defer w.Flush()

	for i := 0; i < iterations; i++ {
		config := Scenario{}
		config.totalWorkers = workers
		config.workerState = make(map[int]map[int]int)
		config.rounds = rounds
		config.failureThreshold = failureThreshold

		// To be changed per scenario case
		numOfFailures := 0
		config.workerState[1] = make(map[int]int)
		config.workerState[1][0] = -1
		totalFailedWorkers++
		numOfFailures++

		config.failures = numOfFailures
		config.networkThresholds = make(map[int]map[int]float64)
		// links to master
		config.networkThresholds[config.totalWorkers] = make(map[int]float64)
		for i := 0; i <= config.totalWorkers; i++ {
			config.networkThresholds[config.totalWorkers][i] = +math.MaxFloat64
		}
		go runCentralSimulation(config, channel)

		// links to master
		config.networkThresholds[config.totalWorkers] = make(map[int]float64)
		for i := 0; i <= config.totalWorkers; i++ {
			config.networkThresholds[config.totalWorkers][i] = +math.MaxFloat64
		}

		// links to workers
		for i := 0; i < config.totalWorkers; i++ {
			config.networkThresholds[i] = make(map[int]float64)
			for j := 0; j <= config.totalWorkers; j++ {
				config.networkThresholds[i][j] = +math.MaxFloat64
			}
		}

		go runDistributedSimulation(config, channel)
	}

	w.Write([]string{"Scheme", "Workers", "Round", "Threshold", "Failures", "Confirmed Failures", "Time to Detect", "Accuracy", "Master Load", "Network Load"})

	// Collecting results from go routines
	for i := 0; i < 2*iterations; i++ {
		result := <-channel
		if result["System"] == 1 {
			// Distributed
			totalTime += result["Time To All Detection"]
			totalFalsePositives += result["False Positives"]
			totalTruePositives += result["True Positives"]
			totalFalseNegatives += result["False Negatives"]
			totalTrueNegatives += result["True Negatives"]
			totalMasterLoad += result["Master Load"]
			totalConfirmedFailures += result["Confirmed Failures"]
			totalNetworkLoad += result["Network Load"]

			accuracy := float64((result["True Positives"]+result["True Negatives"])*100) / float64(result["True Positives"]+result["True Negatives"]+result["False Positives"]+result["False Negatives"])
			w.Write([]string{"Distributed", fmt.Sprint(workers), fmt.Sprint(rounds), fmt.Sprint(failureThreshold), fmt.Sprint(result["Failures"]), fmt.Sprint(result["Confirmed Failures"]), fmt.Sprint(result["Time To All Detection"]), fmt.Sprint(accuracy), fmt.Sprint(float64(result["Master Load"]) / float64(rounds)), fmt.Sprint(float64(result["Network Load"]) / float64(rounds))})
			// fmt.Println("distributed")
			// fmt.Println("False Positives", result["False Positives"])
			// fmt.Println("True Positives", result["True Positives"])
			// fmt.Println("False Negatives", result["False Negatives"])
			// fmt.Println("True Negatives", result["True Negatives"])
		} else {
			// Central
			totalCentralTime += result["Time To All Detection"]
			totalCentralFalsePositives += result["False Positives"]
			totalCentralTruePositives += result["True Positives"]
			totalCentralFalseNegatives += result["False Negatives"]
			totalCentralTrueNegatives += result["True Negatives"]
			totalCentralMasterLoad += result["Master Load"]
			totalCentralConfirmedFailures += result["Confirmed Failures"]
			totalCentralNetworkLoad += result["Network Load"]

			accuracy := float64((result["True Positives"]+result["True Negatives"])*100) / float64(result["True Positives"]+result["True Negatives"]+result["False Positives"]+result["False Negatives"])
			w.Write([]string{"Central", fmt.Sprint(workers), fmt.Sprint(rounds), fmt.Sprint(failureThreshold), fmt.Sprint(result["Failures"]), fmt.Sprint(result["Confirmed Failures"]), fmt.Sprint(result["Time To All Detection"]), fmt.Sprint(accuracy), fmt.Sprint(float64(result["Master Load"]) / float64(rounds)), fmt.Sprint(float64(result["Network Load"]) / float64(rounds))})
			// fmt.Println("central")
			// fmt.Println("False Positives", result["False Positives"])
			// fmt.Println("True Positives", result["True Positives"])
			// fmt.Println("False Negatives", result["False Negatives"])
			// fmt.Println("True Negatives", result["True Negatives"])
		}
	}

	fmt.Println("Total Iterations", iterations)
	fmt.Println("Total Workers", workers)
	fmt.Println("Rounds", rounds)
	fmt.Println("Average failed workers are", float64(totalFailedWorkers)/float64(iterations))

	fmt.Println("\nCentral Scheme Results")
	fmt.Println("Average confirmed failures are", float64(totalCentralConfirmedFailures)/float64(iterations))
	fmt.Println("Average time to detect all failures is", float64(totalCentralTime)/float64(iterations), "rounds or", float64(totalCentralTime)/float64(2*iterations), "RTT")
	fmt.Println("Average accuracy is", float64((totalCentralTruePositives+totalCentralTrueNegatives)*100)/float64(totalCentralTruePositives+totalCentralTrueNegatives+totalCentralFalsePositives+totalCentralFalseNegatives), "%")
	fmt.Println("Average master load is", float64(totalCentralMasterLoad)/(float64(rounds)*float64(iterations)), "messages per round")
	fmt.Println("Average network load is", float64(totalCentralNetworkLoad)/(float64(rounds)*float64(iterations)), "messages per round")

	fmt.Println("\nDistributed Scheme Results")
	fmt.Println("Average confirmed failures are", float64(totalConfirmedFailures)/float64(iterations))
	fmt.Println("Average time to detect all failures is", float64(totalTime)/float64(iterations), "rounds or", float64(totalTime)/float64(2*iterations), "RTT")
	fmt.Println("Average accuracy is", float64((totalTruePositives+totalTrueNegatives)*100)/float64(totalTruePositives+totalTrueNegatives+totalFalsePositives+totalFalseNegatives), "%")
	fmt.Println("Average master load is", float64(totalMasterLoad)/(float64(rounds)*float64(iterations)), "messages per round")
	fmt.Println("Average network load is", float64(totalNetworkLoad)/(float64(rounds)*float64(iterations)), "messages per round")
}
