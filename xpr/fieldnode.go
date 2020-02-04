package xpr

import "nightlycode.de/database/entities/models"

// FieldNode - node representing a field in an entity type
type FieldNode struct {
	name  string
	model *models.EntityModel
}

// Name name of field
func (node *FieldNode) Name() string {
	return node.name
}

// Model model in which field is stored
func (node *FieldNode) Model() *models.EntityModel {
	return node.model
}
