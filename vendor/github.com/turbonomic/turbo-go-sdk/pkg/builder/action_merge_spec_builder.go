package builder

import (
	"fmt"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
)

type ActionMergeSpecBuilder interface {
}

type MoveMergeSpecBuilder struct {
	entityIDs    []string
	targetID     string
	providerType *proto.EntityDTO_EntityType
}

func NewMoveMergeSpecBuilder() *MoveMergeSpecBuilder {
	return &MoveMergeSpecBuilder{}
}

func (mb *MoveMergeSpecBuilder) ForEntities(entityIDs []string) *MoveMergeSpecBuilder {
	mb.entityIDs = entityIDs
	return mb
}

func (mb *MoveMergeSpecBuilder) MergedTo(targetID string) *MoveMergeSpecBuilder {
	mb.targetID = targetID
	return mb
}

func (mb *MoveMergeSpecBuilder) MovingOn(providerType *proto.EntityDTO_EntityType) *MoveMergeSpecBuilder {
	mb.providerType = providerType
	return mb
}

func (mb *MoveMergeSpecBuilder) Build() (*proto.ActionMergeSpec, error) {
	if len(mb.entityIDs) == 0 || mb.targetID == "" {
		return nil, fmt.Errorf("Entity IDs or target ID required for action merge spec")
	}

	if mb.providerType == nil {
		return nil, fmt.Errorf("Provider type required for move merge spec")
	}

	mergeSpec := &proto.ActionMergeSpec{
		EntityIds:    mb.entityIDs,
		TargetEntity: &mb.targetID,
		ActionSpec: &proto.ActionMergeSpec_MoveSpec{
			MoveSpec: &proto.MoveTransformSpec{
				ProviderType: mb.providerType,
			},
		},
	}
	return mergeSpec, nil
}

type ResizeMergeSpecBuilder struct {
	entityIDs   []string
	targetID    string
	commTypes   []proto.CommodityDTO_CommodityType
	changedAttr []proto.ActionItemDTO_CommodityAttribute
}

func NewResizeMergeSpecBuilder() *ResizeMergeSpecBuilder {
	return &ResizeMergeSpecBuilder{}
}

func (rb *ResizeMergeSpecBuilder) ForEntities(entityIDs []string) *ResizeMergeSpecBuilder {
	rb.entityIDs = entityIDs
	return rb
}

func (rb *ResizeMergeSpecBuilder) MergedTo(targetID string) *ResizeMergeSpecBuilder {
	rb.targetID = targetID
	return rb
}

func (rb *ResizeMergeSpecBuilder) ForCommodities(commTypes []proto.CommodityDTO_CommodityType) *ResizeMergeSpecBuilder {
	rb.commTypes = commTypes
	return rb
}

func (rb *ResizeMergeSpecBuilder) ForCommoditiesAndAttributes(commTypes []proto.CommodityDTO_CommodityType,
	changedAttr []proto.ActionItemDTO_CommodityAttribute) *ResizeMergeSpecBuilder {
	rb.commTypes = commTypes
	rb.changedAttr = changedAttr
	return rb
}

func (rb *ResizeMergeSpecBuilder) Build() (*proto.ActionMergeSpec, error) {
	if len(rb.entityIDs) == 0 || rb.targetID == "" {
		return nil, fmt.Errorf("Entity IDs or target ID required for action merge spec")
	}
	if (len(rb.commTypes) == 0) {
		return nil, fmt.Errorf("Commodity types required for resize merge spec")
	}
	mergeSpec := &proto.ActionMergeSpec{
		EntityIds:    rb.entityIDs,
		TargetEntity: &rb.targetID,
		ActionSpec: &proto.ActionMergeSpec_ResizeSpec{
			ResizeSpec: &proto.ResizeTransformSpec{
				CommodityType: rb.commTypes,
				ChangedAttr:   rb.changedAttr,
			},
		},
	}

	return mergeSpec, nil
}

type ProvisionMergeSpecBuilder struct {
	entityIDs []string
	targetID  string
}

func NewProvisionMergeSpecBuilder() *ProvisionMergeSpecBuilder {
	return &ProvisionMergeSpecBuilder{}
}

func (pb *ProvisionMergeSpecBuilder) ForEntities(entityIDs []string) *ProvisionMergeSpecBuilder {
	pb.entityIDs = entityIDs
	return pb
}

func (pb *ProvisionMergeSpecBuilder) MergedTo(targetID string) *ProvisionMergeSpecBuilder {
	pb.targetID = targetID
	return pb
}

func (pb *ProvisionMergeSpecBuilder) Build() (*proto.ActionMergeSpec, error) {
	if len(pb.entityIDs) == 0 || pb.targetID == "" {
		return nil, fmt.Errorf("Entity IDs or target ID required for action merge spec")
	}

	mergeSpec := &proto.ActionMergeSpec{
		EntityIds:    pb.entityIDs,
		TargetEntity: &pb.targetID,
		ActionSpec: &proto.ActionMergeSpec_ProvisionSpec{
			ProvisionSpec: &proto.ProvisionTransformSpec{},
		},
	}

	return mergeSpec, nil
}
