package entities

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/entities/statements"
)

// IEntityManager - manages access to database with fluent statements
type IEntityManager interface {

	// Transaction starts a transaction using the underlying db connection
	Transaction() (*sql.Tx, error)

	// loads entities from the database
	LoadEntities(model *models.EntityModel) *statements.LoadStatement

	// loads data from a table
	Load(model *models.EntityModel, fields ...interface{}) *statements.LoadStatement

	// inserts data into a table
	Insert(model *models.EntityModel) *statements.InsertStatement

	// updates data of a table
	Update(model *models.EntityModel) *statements.UpdateStatement

	// selects raw values from the database
	Select(fields ...interface{}) *statements.LoadStatement

	// deletes rows from a database
	Delete(model *models.EntityModel) *statements.DeleteStatement

	// updates schema of a table in database (or creates it)
	UpdateSchema(model *models.EntityModel) error
}

// EntityManager manages access to database with fluent statements using a database connection
type EntityManager struct {
	connection     *sql.DB                    // connection to database
	connectioninfo connection.IConnectionInfo // driver specific information about database
	schemaupdater  *SchemaUpdater
}

// NewEntitymanager - creates a new entitymanager
func NewEntitymanager(connection *sql.DB, connectioninfo connection.IConnectionInfo) *EntityManager {
	return &EntityManager{
		connection:     connection,
		connectioninfo: connectioninfo,
		schemaupdater:  &SchemaUpdater{}}
}

// Transaction starts a transaction using the underlying db connection
func (manager *EntityManager) Transaction() (*sql.Tx, error) {
	return manager.connection.BeginTx(context.Background(), &sql.TxOptions{})
}

// LoadEntities loads entities from the database
//
// **Parameters**
//   - model: model of entity for which to insert data
//
// **Returns**
//   - LoadEntityStatement: statement to use to prepare load entity operation
func (manager *EntityManager) LoadEntities(model *models.EntityModel) *statements.LoadStatement {
	return statements.NewLoadStatement(manager.connection, manager.connectioninfo).Model(model)
}

// Load creates a statement used to load data from the database
//
// **Parameters**
//   - model: model of entity for which to insert data
//   - fields: specifies fields to load from database
//
// **Returns**
//   - LoadStatement: statement to use to prepare load operation
func (manager *EntityManager) Load(model *models.EntityModel, fields ...interface{}) *statements.LoadStatement {
	return manager.Select(fields...).Table(model.Table)
}

// Select loads pure values from the database
//
// **Parameters**
//   - fields: specifies fields to load from database
//
// **Returns**
//   - LoadStatement: statement to use to prepare load operation
func (manager *EntityManager) Select(fields ...interface{}) *statements.LoadStatement {
	statement := statements.NewLoadStatement(manager.connection, manager.connectioninfo)

	if len(fields) > 0 {
		statement.Fields(fields...)
	}
	return statement
}

// Insert creates an insert statement used to insert entity data into a database
//
// **Parameters**
//   - model: model of entity for which to insert data
//
// **Returns**
//   - InsertStatement: statement to use to prepare insert operation
func (manager *EntityManager) Insert(model *models.EntityModel) *statements.InsertStatement {
	return statements.NewInsertStatement(model, manager.connection, manager.connectioninfo)
}

// Update creates an update statement used to update entity data in the database
//
// **Parameters**
//   - model: model of entity of which to update data
//
// **Returns**
//   - *UpdateStatement: statement to use to prepare update operation
func (manager *EntityManager) Update(model *models.EntityModel) *statements.UpdateStatement {
	return statements.NewUpdateStatement(model, manager.connection, manager.connectioninfo)
}

// Delete creates a delete statement used to remove entities from the database
//
// **Parameters**
//   - model: model of entity of which to update data
//
// **Returns**
//   - *DeleteStatement: statement to use to prepare delete operation
func (manager *EntityManager) Delete(model *models.EntityModel) *statements.DeleteStatement {
	return statements.NewDeleteStatement(model, manager.connection, manager.connectioninfo)
}

// Exists determines whether an entity has a table or view in database
//
// **Parameters**
//   - model: model of entity to check for
//
// **Returns**
//   - bool: true if entity has a table or view in database
//   - error: error information when database command resultet in error
func (manager *EntityManager) Exists(model *models.EntityModel) (bool, error) {
	return manager.connectioninfo.ExistsTableOrView(manager.connection, model.Table)
}

// Create creates a new table or view for an entity in database. This is only to be used if the table does not exists already.
//
// **Parameters**
//   - model: model of entity to check for
func (manager *EntityManager) Create(model *models.EntityModel) error {
	_, err := statements.NewCreateStatement(model, manager.connection, manager.connectioninfo).Prepare().Execute()
	if err != nil {
		return err
	}

	indices := model.Indices()
	if len(indices) > 0 {

		for _, index := range indices {
			_, err = statements.NewCreateIndexStatement(model, index, manager.connection, manager.connectioninfo).Prepare().Execute()
			if err != nil {
				return err
			}
		}

	}

	return nil
}

// UpdateSchema updates the schema of an entity in database
//
// **Parameters**
//   - model: model of entity to update in database
//
// **Returns**
//   - error: error if any occured, nil otherwise
func (manager *EntityManager) UpdateSchema(model *models.EntityModel) error {
	exists, err := manager.Exists(model)
	if err != nil {
		return err
	}

	if !exists {
		err := manager.Create(model)
		if err != nil {
			return err
		}

		return nil
	}

	schema, err := manager.connectioninfo.GetSchema(manager.connection, model.Table)
	if err != nil {
		return fmt.Errorf("Unable to get schema information: %s", err.Error())
	}

	updater := &SchemaUpdater{
		connection:     manager.connection,
		connectioninfo: manager.connectioninfo}

	switch schema.Type() {
	case models.SchemaTypeTable:
		err = updater.UpdateTable(model, schema.(*models.Table))
	case models.SchemaTypeView:
		err = updater.UpdateView(model, schema.(*models.View))
	default:
		err = errors.New("Unknown schema type")
	}

	if err != nil {
		return fmt.Errorf("Unable to update schema: %s", err.Error())
	}

	return nil
}
