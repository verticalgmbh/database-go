package entities

import (
	"database/sql"
	"fmt"

	"github.com/verticalgmbh/collections-go/coll"
	"github.com/verticalgmbh/database-go/entities/statements"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

// SchemaUpdater updates a schema in database
type SchemaUpdater struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
}

// UpdateView updates a view in database
//
// **Parameters**
//   - newmodel: model of updated view
//   - oldmodel: schema of view currently stored in database
//
// **Result**
//   - error: error if any occured
func (updater *SchemaUpdater) UpdateView(newmodel *models.EntityModel, oldschema *models.View) error {
	_, err := updater.connection.Exec(fmt.Sprintf("DROP VIEW %s", oldschema.SchemaName()))
	if err != nil {
		return fmt.Errorf("Error updating view '%s': %s", oldschema.SchemaName(), err.Error())
	}

	_, err = updater.connection.Exec(newmodel.ViewSQL())
	if err != nil {
		return fmt.Errorf("Error updating view '%s': %s", oldschema.SchemaName(), err.Error())
	}

	return nil
}

func (updater *SchemaUpdater) getMissingColumns(newmodel *models.EntityModel, oldschema *models.Table) []*models.ColumnDescriptor {
	var missing []*models.ColumnDescriptor

	for _, currentcolumn := range newmodel.Columns() {
		exists := false
		for _, newcolumn := range oldschema.Columns() {
			if newcolumn.Name() == currentcolumn.Name() {
				exists = true
				break
			}
		}

		if !exists {
			missing = append(missing, currentcolumn)
		}
	}

	return missing
}

func (updater *SchemaUpdater) areTypesEqual(lhs string, rhs string) bool {
	if lhs == "TEXT" || lhs == "VARCHAR" {
		return rhs == "TEXT" || rhs == "VARCHAR"
	}

	return lhs == rhs
}

func (updater *SchemaUpdater) getAlteredColumns(newmodel *models.EntityModel, oldschema *models.Table) ([]*models.ColumnDescriptor, []string) {
	var altered []*models.ColumnDescriptor
	var obsolete []string

	for _, newcolumn := range oldschema.Columns() {
		var existing *models.ColumnDescriptor

		for _, currentcolumn := range newmodel.Columns() {
			if newcolumn.Name() == currentcolumn.Name() {
				existing = currentcolumn
				break
			}
		}

		if existing == nil {
			obsolete = append(obsolete, existing.Name())
			continue
		}

		if updater.areTypesEqual(newcolumn.DBType(), updater.connectioninfo.GetDatabaseType(existing.DataType())) || newcolumn.IsPrimaryKey() != existing.IsPrimaryKey() || newcolumn.IsAutoIncrement() != existing.IsAutoIncrement() || newcolumn.IsUnique() != existing.IsUnique() || newcolumn.IsNotNull() != existing.IsNotNull() {
			altered = append(altered, existing)
		}
	}

	return altered, obsolete
}

func (updater *SchemaUpdater) hasMissingUniques(missing []*models.ColumnDescriptor) bool {
	for _, column := range missing {
		if column.IsUnique() || column.IsPrimaryKey() {
			return true
		}
	}

	return false
}

func (updater *SchemaUpdater) indexEqual(lhs *models.IndexDescriptor, rhs *models.IndexDescriptor) bool {
	if len(lhs.Columns()) != len(rhs.Columns()) {
		return false
	}

	return coll.AllString(lhs.Columns(), func(oldcolumn string) bool {
		return coll.AnyString(rhs.Columns(), func(newcolumn string) bool {
			return oldcolumn == newcolumn
		})
	})
}

func (updater *SchemaUpdater) indexSequenceEqual(oldindices []*models.IndexDescriptor, newindices []*models.IndexDescriptor) bool {
	if len(oldindices) != len(newindices) {
		return false
	}

	for _, index := range oldindices {

		for _, candidate := range newindices {
			// since the name of an index is irrelevant for functionality
			// it isn't checked here

			if !updater.indexEqual(index, candidate) {
				return false
			}
		}
	}

	return true
}

func (updater *SchemaUpdater) containsIndex(index *models.IndexDescriptor, indexcollection []*models.IndexDescriptor) bool {
	for _, item := range indexcollection {
		if updater.indexEqual(index, item) {
			return true
		}
	}

	return false
}

func (updater *SchemaUpdater) recreateTable(newmodel *models.EntityModel, oldschema *models.Table) error {
	/*transaction, err := updater.connection.Begin()
	if err != nil {
		return fmt.Errorf("Error creating transaction: %s", err.Error())
	}*/

	backupname := fmt.Sprintf("%s_original", newmodel.Table)

	schemas, err := updater.connectioninfo.GetSchemas(updater.connection)

	exists, err := updater.connectioninfo.ExistsTableOrView(updater.connection, backupname)
	if err != nil {
		return fmt.Errorf("Error checking for table: %s", err.Error())
	}

	schemas, err = updater.connectioninfo.GetSchemas(updater.connection)

	if exists {
		_, err = statements.NewDropTable(updater.connection, updater.connectioninfo, backupname).Prepare().Execute()
		if err != nil {
			return fmt.Errorf("Error removing old backup table: %s", err.Error())
		}
	}

	schemas, err = updater.connectioninfo.GetSchemas(updater.connection)

	_, err = statements.NewRenameTable(updater.connection, updater.connectioninfo, newmodel.Table, backupname).Prepare().Execute()
	if err != nil {
		return fmt.Errorf("Error renaming table: %s", err.Error())
	}

	schemas, err = updater.connectioninfo.GetSchemas(updater.connection)

	_, err = statements.NewCreateStatement(newmodel, updater.connection, updater.connectioninfo).Prepare().Execute()
	if err != nil {
		return fmt.Errorf("Error creating new table: %s", err.Error())
	}

	schemas, err = updater.connectioninfo.GetSchemas(updater.connection)

	var remaining []*models.ColumnDescriptor

	coll.AddToWhere(oldschema.Columns(), func(item interface{}) bool {
		olditem := item.(*models.ColumnDescriptor)
		return coll.Any(newmodel.Columns(), func(newitem interface{}) bool {
			columnitem := newitem.(*models.ColumnDescriptor)
			return columnitem.Name() == olditem.Name()
		})
	}, &remaining)

	coll.AddToWhere(newmodel.Columns(), func(item interface{}) bool {
		newitem := item.(*models.ColumnDescriptor)
		return newitem.IsNotNull() && !newitem.IsAutoIncrement() && newitem.DefaultValue() == "" && coll.All(oldschema.Columns(), func(iolditem interface{}) bool {
			olditem := iolditem.(*models.ColumnDescriptor)
			return olditem.Name() != newitem.Name()
		})
	}, &remaining)

	_, err = statements.NewInsertLoad(newmodel, updater.connection, updater.connectioninfo).Columns(remaining...).Load(
		statements.NewLoadStatement(backupname, updater.connection, updater.connectioninfo).Columns(remaining)).Prepare().Execute()
	if err != nil {
		return fmt.Errorf("Error inserting existing data into new table: %s", err.Error())
	}

	schemas, err = updater.connectioninfo.GetSchemas(updater.connection)

	_, err = statements.NewDropTable(updater.connection, updater.connectioninfo, backupname).Prepare().Execute()
	if err != nil {
		return fmt.Errorf("Error removing old backup table: %s", err.Error())
	}

	schemas, err = updater.connectioninfo.GetSchemas(updater.connection)
	if schemas != nil {
		return nil
	}
	/*err = transaction.Commit()
	if err != nil {
		return fmt.Errorf("Error commiting transaction: %s", err.Error())
	}*/
	return nil
}

func (updater *SchemaUpdater) updateIndices(newmodel *models.EntityModel, oldschema *models.Table, transaction *sql.Tx) error {
	for _, index := range oldschema.Indices() {
		existing := coll.FirstOrDefault(newmodel.Indices(), func(iitem interface{}) bool {
			item := iitem.(*models.ColumnDescriptor)
			return item.Name() == index.Name()
		}).(*models.IndexDescriptor)

		if existing == nil {
			_, err := statements.NewDropIndex(updater.connection, updater.connectioninfo, newmodel, index.Name()).Prepare().ExecuteTransaction(transaction)
			if err != nil {
				return fmt.Errorf("Error dropping index: %s", err.Error())
			}
		} else {
			if !updater.indexEqual(index, existing) {
				_, err := statements.NewDropIndex(updater.connection, updater.connectioninfo, newmodel, index.Name()).Prepare().ExecuteTransaction(transaction)
				if err != nil {
					return fmt.Errorf("Error dropping index: %s", err.Error())
				}

				_, err = statements.NewCreateIndexStatement(newmodel, index, updater.connection, updater.connectioninfo).Prepare().ExecuteTransaction(transaction)
				if err != nil {
					return fmt.Errorf("Error creating new index: %s", err.Error())
				}
			}
		}
	}

	for _, index := range newmodel.Indices() {
		if !coll.Any(oldschema.Indices(), func(iitem interface{}) bool {
			item := iitem.(*models.IndexDescriptor)
			return updater.indexEqual(index, item)
		}) {
			_, err := statements.NewCreateIndexStatement(newmodel, index, updater.connection, updater.connectioninfo).Prepare().ExecuteTransaction(transaction)
			if err != nil {
				return fmt.Errorf("Error creating new index: %s", err.Error())
			}
		}
	}

	return nil
}

// UpdateTable updates a table in database
//
// **Parameters**
//   - newmodel: model of updated entity
//   - oldmodel: schema of entity currently stored in database
//
// **Result**
//   - error: error if any occured
func (updater *SchemaUpdater) UpdateTable(newmodel *models.EntityModel, oldschema *models.Table) error {
	missing := updater.getMissingColumns(newmodel, oldschema)
	altered, obsolete := updater.getAlteredColumns(newmodel, oldschema)

	recreatetable := len(obsolete) > 0 || len(altered) > 0 || updater.hasMissingUniques(missing) || !updater.indexSequenceEqual(oldschema.Indices(), newmodel.Indices()) || !updater.indexSequenceEqual(oldschema.Uniques(), newmodel.Uniques())

	if recreatetable {
		err := updater.recreateTable(newmodel, oldschema)
		if err != nil {
			return fmt.Errorf("Error recreating table: %s", err.Error())
		}
	} else {
		transaction, err := updater.connection.Begin()
		if err != nil {
			return fmt.Errorf("Error starting transaction: %s", err.Error())
		}

		if len(missing) > 0 {
			for _, column := range missing {
				statements.NewAddColumnStatement(updater.connection, updater.connectioninfo, newmodel, column).Prepare().ExecuteTransaction(transaction)
			}
		}

		// TODO drop obsolete uniques for postgres (sqlite does not support dropping uniques so table does get recreated there anyways)
		for _, index := range newmodel.Indices() {
			if !updater.containsIndex(index, oldschema.Indices()) {
				statements.NewAddUnique(updater.connection, updater.connectioninfo, newmodel, index).Prepare().ExecuteTransaction(transaction)
			}
		}

		err = updater.updateIndices(newmodel, oldschema, transaction)
		if err != nil {
			return fmt.Errorf("Error updating indices: %s", err.Error())
		}

		err = transaction.Commit()
		if err != nil {
			return fmt.Errorf("Error commiting transaction: %s", err.Error())
		}
	}

	return nil
}
