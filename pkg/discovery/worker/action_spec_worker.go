package worker

import (
	"github.com/golang/glog"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/turbonomic/kubeturbo/pkg/discovery/repository"
	"github.com/turbonomic/turbo-go-sdk/pkg/builder"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
)

const (
	k8sActionMergeSpecWorkerID string = "k8sActionMergeSpecWorkerID"
)

// Converts the container specs to action merge specs
type k8sActionSpecWorker struct {
	id       string
	targetId string
	cluster  *repository.ClusterSummary
}

func Newk8sActionSpecWorker(cluster *repository.ClusterSummary,
	targetId string) *k8sActionSpecWorker {
	return &k8sActionSpecWorker{
		cluster:  cluster,
		id:       k8sActionMergeSpecWorkerID,
		targetId: targetId,
	}
}

// Action merge spec will use the entity groups consolidated by the result_collector
// to create the action merge spec for the container groups
func (worker *k8sActionSpecWorker) Do(containerSpecList []*repository.ContainerSpec) ([]*proto.ActionMergeSpec, error) {

	var specDTOS []*proto.ActionMergeSpec

	containerMap := make(map[string][]string)
	controllerMap := make(map[string][]string)
	for _, containerSpec := range containerSpecList {
		containerSpecId := containerSpec.ContainerSpecId
		_, exists := containerMap[containerSpecId]
		if !exists {
			containerMap[containerSpecId] = containerSpec.ContainerUIDs
		} else {
			containerMap[containerSpecId] = append(containerMap[containerSpecId], containerSpec.ContainerUIDs...)
		}
		controllerId := containerSpec.ControllerUID
		controllerMap[controllerId] = append(controllerMap[controllerId], containerSpec.ContainerSpecId)
	}

	for controllerId, containerSpecList := range controllerMap {
		for _, containerSpecId := range containerSpecList {
			containerList := containerMap[containerSpecId]

			glog.Infof("Workload Controller: %s --> Specs :%v",
										controllerId, containerSpecList)

			comms := []proto.CommodityDTO_CommodityType{proto.CommodityDTO_VCPU, proto.CommodityDTO_VMEM}
			spec := builder.NewResizeMergeSpecBuilder()

			spec.ForEntities(containerList).
				ForCommodities(comms).
				MergedTo(containerSpecId).
				AggregatedTo(controllerId)

			specDTO, err := spec.Build()

			if err != nil {
				glog.Errorf("%++v", err)
				continue
			}
			glog.Infof("SPEC DTO: %++v", protobuf.MarshalTextString(specDTO))
			specDTOS = append(specDTOS, specDTO)
		}

	}

	//for _, entityGroup := range entityGroupList {
	//	glog.Infof("### Parent: %s:%s --->", entityGroup.ParentKind, entityGroup.ParentName)
	//
	//	containerGroups := entityGroup.ContainerGroups
	//
	//	for containerName, containerSet := range containerGroups {
	//		glog.Infof(" ********** Container Group : %s:%v", containerName, containerSet)
	//
	//		comms := []proto.CommodityDTO_CommodityType{proto.CommodityDTO_VCPU, proto.CommodityDTO_VMEM}
	//		spec := builder.NewResizeMergeSpecBuilder()
	//
	//		mergeEntity := fmt.Sprintf("ContainerSpec:%s", containerName)
	//		spec.ForEntities(containerSet).
	//			ForCommodities(comms).
	//			MergedTo(mergeEntity)
	//
	//		specDTO, err := spec.Build()
	//		aggregationEntityType := 65
	//		aggregationEntity := fmt.Sprintf("%d:%v", aggregationEntityType, )
	//		specDTO.AggregationEntity = &aggregationEntity	//entityGroup.GroupId
	//		if err != nil {
	//			glog.Errorf("%++v", err)
	//			continue
	//		}
	//		glog.Infof("SPEC DTO: %++v", protobuf.MarshalTextString(specDTO))
	//		specDTOS = append(specDTOS, specDTO)
	//	}
	//}

	return specDTOS, nil
}
