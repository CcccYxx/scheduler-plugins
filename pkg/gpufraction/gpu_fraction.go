package gpufraction


import (
    "context"
    "fmt"
	"strconv"

    v1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/runtime"
    utilruntime "k8s.io/apimachinery/pkg/util/runtime"
    "k8s.io/client-go/informers"
    clientgoscheme "k8s.io/client-go/kubernetes/scheme"
    corelisters "k8s.io/client-go/listers/core/v1"
    "k8s.io/client-go/util/retry"
    "k8s.io/kubernetes/pkg/scheduler/framework"

    "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

const (
    Name             = "GpuFractionPlugin"
    GPUResource      = "nvidia.com/gpu"
    GPUMemResource   = "nvidia.com/gpumem"
    GPUCoresResource = "nvidia.com/gpucores"
    VolumeName       = "libvgpu"
    VolumeMountPath  = "/etc/libvgpu"
    PreloadLibrary   = "/etc/libvgpu/libvgpu.so"
    MemoryLimitEnv   = "CUDA_DEVICE_MEMORY_LIMIT"
    SMLimitEnv       = "CUDA_DEVICE_SM_LIMIT"
    ConfigMapName    = "libvgpu"
)

var scheme = runtime.NewScheme()

func init() {
    utilruntime.Must(clientgoscheme.AddToScheme(scheme))
    utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

type GpuFractionPlugin struct {
    handle    framework.Handle
    podLister corelisters.PodLister
}

var _ framework.PreFilterPlugin = &GpuFractionPlugin{}

func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
    informerFactory := informers.NewSharedInformerFactory(handle.ClientSet(), 0)
    podLister := informerFactory.Core().V1().Pods().Lister()

    informerFactory.Start(ctx.Done())
    informerFactory.WaitForCacheSync(ctx.Done())

    return &GpuFractionPlugin{
        handle:    handle,
        podLister: podLister,
    }, nil
}

func (pl *GpuFractionPlugin) Name() string {
    return Name
}

func (pl *GpuFractionPlugin) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
    for i, container := range pod.Spec.Containers {
        if needsGPUResources(container.Resources) {
            resources := &container.Resources
			// TODO: mem and core requests do not need to co-exist
            memLimitQuantity := resources.Requests[GPUMemResource]
            smLimitQuantity := resources.Requests[GPUCoresResource]

            delete(resources.Requests, GPUMemResource)
            delete(resources.Requests, GPUCoresResource)
            delete(resources.Limits, GPUMemResource)
            delete(resources.Limits, GPUCoresResource)

            memLimit := memLimitQuantity.Value() / 1024
            smLimit := smLimitQuantity.Value()

            pod.Spec.Containers[i].VolumeMounts = append(container.VolumeMounts, v1.VolumeMount{
                Name:      VolumeName,
                MountPath: VolumeMountPath,
            })
            pod.Spec.Containers[i].Env = append(container.Env,
                v1.EnvVar{Name: "LD_PRELOAD", Value: PreloadLibrary},
                v1.EnvVar{Name: MemoryLimitEnv, Value: fmt.Sprintf("%dg", memLimit)},
                v1.EnvVar{Name: SMLimitEnv, Value: strconv.Itoa(int(smLimit))},
            )
        }
    }

    pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
        Name: VolumeName,
        VolumeSource: v1.VolumeSource{
            ConfigMap: &v1.ConfigMapVolumeSource{
                LocalObjectReference: v1.LocalObjectReference{Name: ConfigMapName},
            },
        },
    })

    if err := pl.updatePod(pod); err != nil {
        return nil, framework.NewStatus(framework.Error, fmt.Sprintf("failed to update pod: %v", err))
    }

    return &framework.PreFilterResult{}, framework.NewStatus(framework.Success, "")
}

func (pl *GpuFractionPlugin) PreFilterExtensions() framework.PreFilterExtensions {
    return pl
}

func (pl *GpuFractionPlugin) updatePod(pod *v1.Pod) error {
    return retry.RetryOnConflict(retry.DefaultRetry, func() error {
        updatedPod, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
        if err != nil {
            return err
        }

        updatedPod.Spec = pod.Spec

        _, err = pl.handle.ClientSet().CoreV1().Pods(updatedPod.Namespace).Update(context.TODO(), updatedPod, metav1.UpdateOptions{})
        return err
    })
}

func needsGPUResources(resources v1.ResourceRequirements) bool {
    _, gpuExists := resources.Requests[GPUResource]
    _, gpuMemExists := resources.Requests[GPUMemResource]
    _, gpuCoresExists := resources.Requests[GPUCoresResource]
	// TODO: mem or core does not need to co-exist
    return gpuExists && gpuMemExists && gpuCoresExists
}

// PlaceHolder functions for scheduler framework
func (pl *GpuFractionPlugin) AddPod(ctx context.Context, cycleState *framework.CycleState, podToSchedule *v1.Pod, podToAdd *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

func (pl *GpuFractionPlugin) RemovePod(ctx context.Context, cycleState *framework.CycleState, podToSchedule *v1.Pod, podToRemove *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

func (pl *GpuFractionPlugin) PostFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, m framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	return nil, framework.NewStatus(framework.Success, "")
}

func (pl *GpuFractionPlugin) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

func (pl *GpuFractionPlugin) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	return
}
