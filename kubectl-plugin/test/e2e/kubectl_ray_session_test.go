package e2e

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"syscall"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Calling ray plugin `session` command", Ordered, func() {
	var namespace string

	BeforeEach(func() {
		namespace = createTestNamespace()
		deployTestRayCluster(namespace)
		DeferCleanup(func() {
			deleteTestNamespace(namespace)
			namespace = ""
		})
	})

	It("succeed in forwarding RayCluster and should be able to cancel", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cmd := exec.CommandContext(ctx, "kubectl", "ray", "session", "--namespace", namespace, "raycluster-kuberay")
		// Set the command to run in a new process group
		// This is necessary to ensure that the signal is sent to all child processes
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		err := cmd.Start()
		Expect(err).NotTo(HaveOccurred())
		// Make sure we kill the whole process group, even if the test is failure
		defer cleanUpCmdProcessAndCheckPortForwarding(cmd)

		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		// Send a request to localhost:8265, it should succeed
		Eventually(func() error {
			_, err := exec.CommandContext(ctx, "curl", "http://localhost:8265").CombinedOutput()
			return err
		}, 3*time.Second, 500*time.Millisecond).ShouldNot(HaveOccurred())
		pgid, err := syscall.Getpgid(cmd.Process.Pid)
		Expect(err).NotTo(HaveOccurred())

		// Send Interrupt signal to the process group
		// This will also send the signal to all child processes
		err = syscall.Kill(-pgid, syscall.SIGINT)
		Expect(err).NotTo(HaveOccurred())

		select {
		case err = <-done:
			if err != nil {
				exitErr := &exec.ExitError{}
				Expect(errors.As(err, &exitErr)).To(BeTrue())
				ws := exitErr.Sys().(syscall.WaitStatus)
				Expect(ws.Signal()).To(Equal(syscall.SIGINT))
			}
		case <-time.After(3 * time.Second):
			Fail("kubectl ray session did not terminate after SIGINT")
		}
	})

	It("should reconnect after pod connection is lost", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sessionCmd := exec.CommandContext(ctx, "kubectl", "ray", "session", "--namespace", namespace, "raycluster-kuberay")
		// Set the command to run in a new process group
		// This is necessary to ensure that the signal is sent to all child processes
		sessionCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		sessionCmd.Stdout = GinkgoWriter
		sessionCmd.Stderr = GinkgoWriter
		err := sessionCmd.Start()
		Expect(err).NotTo(HaveOccurred())

		// Make sure we kill the whole process group, even if the test is failure
		defer cleanUpCmdProcessAndCheckPortForwarding(sessionCmd)
		GinkgoWriter.Printf("[%s] session started pid=%d namespace=%s\n", time.Now().Format(time.RFC3339Nano), sessionCmd.Process.Pid, namespace)

		// Send a request to localhost:8265, it should succeed
		Eventually(func() error {
			_, err := exec.CommandContext(ctx, "curl", "http://localhost:8265").CombinedOutput()
			return err
		}, 3*time.Second, 500*time.Millisecond).ShouldNot(HaveOccurred())

		// Get the current head pod name
		cmd := exec.Command("kubectl", "get", "--namespace", namespace, "raycluster/raycluster-kuberay", "-o", "jsonpath={.status.head.podName}")
		output, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		oldPodName := strings.TrimSpace(string(output))
		Expect(oldPodName).NotTo(BeEmpty())
		GinkgoWriter.Printf("[%s] old head pod=%q\n", time.Now().Format(time.RFC3339Nano), oldPodName)
		var newPodName string

		// Delete the pod
		cmd = exec.Command("kubectl", "delete", "--namespace", namespace, "pod", oldPodName)
		output, err = cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), "kubectl delete output: %s", string(output))
		GinkgoWriter.Printf("[%s] delete completed output=%q\n", time.Now().Format(time.RFC3339Nano), string(output))

		// Wait for the current replacement pod to be ready. Resolve the current head
		// pod on every attempt because it may be replaced again while becoming ready.
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "--namespace", namespace, "raycluster/raycluster-kuberay", "-o", "jsonpath={.status.head.podName}")
			output, err := cmd.CombinedOutput()
			g.Expect(err).NotTo(HaveOccurred(), "failed to get the current head pod: %s", string(output))

			candidatePodName := strings.TrimSpace(string(output))
			g.Expect(candidatePodName).NotTo(BeEmpty(), "current head pod name is empty")
			g.Expect(candidatePodName).NotTo(Equal(oldPodName), "head pod has not changed")

			cmd = exec.Command(
				"kubectl", "get", "--namespace", namespace, "pod", candidatePodName,
				"-o", "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}",
			)
			output, err = cmd.CombinedOutput()
			g.Expect(err).NotTo(HaveOccurred(), "failed to get replacement head pod %q: %s", candidatePodName, string(output))
			g.Expect(strings.TrimSpace(string(output))).To(Equal("True"), "replacement head pod %q is not ready", candidatePodName)

			newPodName = candidatePodName
		}, 180*time.Second, 1*time.Second).Should(Succeed())
		GinkgoWriter.Printf("[%s] replacement head pod ready=%q\n", time.Now().Format(time.RFC3339Nano), newPodName)

		// Send a request to localhost:8265, it should succeed
		Eventually(func() error {
			_, err := exec.CommandContext(ctx, "curl", "http://localhost:8265").CombinedOutput()
			return err
		}, 60*time.Second, 500*time.Millisecond).ShouldNot(HaveOccurred())
	})

	It("should not succeed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cmd := exec.CommandContext(ctx, "kubectl", "ray", "session", "--namespace", namespace, "fakeclustername")
		output, err := cmd.CombinedOutput()

		Expect(err).To(HaveOccurred())
		Expect(output).ToNot(ContainElements("fakeclustername"))
	})

	It("should not leak the port-forward child when only the session PID is signaled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cmd := exec.CommandContext(ctx, "kubectl", "ray", "session", "--namespace", namespace, "raycluster-kuberay")
		// Run in its own process group so the deferred cleanup can kill the group
		// without signaling the test process itself.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		err := cmd.Start()
		Expect(err).NotTo(HaveOccurred())
		defer cleanUpCmdProcessAndCheckPortForwarding(cmd)

		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		Eventually(func() error {
			_, err := exec.CommandContext(ctx, "curl", "http://localhost:8265").CombinedOutput()
			return err
		}, 30*time.Second, 500*time.Millisecond).ShouldNot(HaveOccurred())

		// Signal the session PID only, not the process group.
		err = syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
		Expect(err).NotTo(HaveOccurred())

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			Fail("kubectl ray session did not terminate after SIGTERM")
		}
	})
})

func cleanUpCmdProcessAndCheckPortForwarding(cmd *exec.Cmd) {
	pgid, _ := syscall.Getpgid(cmd.Process.Pid)
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	// prevent zombie process
	_ = cmd.Wait()
	// Check if the port-forwarding process is still running
	_, err := exec.CommandContext(context.Background(), "lsof", "-i", ":8265").CombinedOutput()
	Expect(err).To(HaveOccurred())
}
