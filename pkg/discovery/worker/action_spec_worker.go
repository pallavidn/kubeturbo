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

// Converts the consoliated group object to action merge spec
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
func (worker *k8sActionSpecWorker) Do(entityGroupList []*repository.EntityGroup,
) ([]*proto.ActionMergeSpec, error) {

	var specDTOS []*proto.ActionMergeSpec
	for _, entityGroup := range entityGroupList {
		glog.Infof("### Parent: %s:%s --->", entityGroup.ParentKind, entityGroup.ParentName)

		containerGroups := entityGroup.ContainerGroups

		for containerName, containerSet := range containerGroups {
			glog.Infof(" ********** Container Group : %s:%v", containerName, containerSet)

			comms := []proto.CommodityDTO_CommodityType{proto.CommodityDTO_VCPU, proto.CommodityDTO_VMEM}
			spec := builder.NewResizeMergeSpecBuilder()
			spec.ForEntities(containerSet).
				ForCommodities(comms).
				MergedTo(entityGroup.GroupId)

			specDTO, err := spec.Build()
			if err != nil {
				glog.Errorf("%++v", err)
				continue
			}
			glog.Infof("SPEC DTO: %++v", protobuf.MarshalTextString(specDTO))
			specDTOS = append(specDTOS, specDTO)
		}
	}

	return specDTOS, nil
}
