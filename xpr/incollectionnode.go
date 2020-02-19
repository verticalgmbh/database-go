package xpr

// InCollectionNode predicate node which expects an item to be part of a collection
//                  in sql this translates to an 'item IN (collection)' statement
type InCollectionNode struct {
	item       interface{}
	collection []interface{}
}

// Item item to check for in the collection
func (node *InCollectionNode) Item() interface{} {
	return node.item
}

// Collection collection of items to be checked
func (node *InCollectionNode) Collection() []interface{} {
	return node.collection
}
