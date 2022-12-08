package dyndb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"strings"
	"time"
)

type dialect struct{ dsc.DatastoreDialect }

var maxWaitTime = 2 * time.Minute

//GetKeyName returns a name of column name that is a key, or coma separated list if complex key
func (d *dialect) GetKeyName(manager dsc.Manager, datastore, table string) string {
	var result = make([]string, 0)
	keyAttributes, err := d.getKeyAttributes(manager, table)
	if err != nil {
		return ""
	}
	for _, item := range keyAttributes {
		result = append(result, *item.AttributeName)
	}
	return strings.Join(result, ",")
}

func (d *dialect) getKeyAttributes(manager dsc.Manager, table string) ([]*dynamodb.KeySchemaElement, error) {
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	db, err := asDatabase(connection)
	if err != nil {
		return nil, err
	}
	output, err := db.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &table,
	})
	if err != nil {
		return nil, err
	}
	return output.Table.KeySchema, nil
}

func (d *dialect) GetColumns(manager dsc.Manager, datastore, table string) ([]dsc.Column, error) {
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	db, err := asDatabase(connection)
	if err != nil {
		return nil, err
	}
	output, err := db.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &table,
	})
	if err != nil {
		return nil, err
	}
	keys := make(map[string]bool)
	var result = make([]dsc.Column, 0)
	for _, item := range output.Table.AttributeDefinitions {
		keys[*item.AttributeName] = true
		result = append(result, dsc.NewSimpleColumn(*item.AttributeName, *item.AttributeType))
	}
	if queryOutput, err := db.Query(&dynamodb.QueryInput{
		TableName: aws.String(table),
		Limit:     aws.Int64(1),
	}); err == nil && len(queryOutput.Items) > 0 {
		for k, v := range queryOutput.Items[0] {
			if keys[k] {
				continue
			}
			result = append(result, dsc.NewSimpleColumn(k, getAttributeType(v)))
		}
	}
	return result, nil
}

func (d *dialect) DropTable(manager dsc.Manager, datastore string, table string) error {
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()
	db, err := asDatabase(connection)
	if err != nil {
		return err
	}
	_, err = db.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: &table,
	})
	waitForTableDeletion(db, table)
	return err
}

func waitForTableDeletion(db *dynamodb.DynamoDB, table string) {
	startTime := time.Now()
	for time.Now().Sub(startTime) < maxWaitTime {
		_, err := db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (d *dialect) CreateTable(manager dsc.Manager, datastore string, table string, specification interface{}) error {
	input := &dynamodb.CreateTableInput{}
	descriptor := manager.TableDescriptorRegistry().Get(table)
	if descriptor.SchemaURL != "" {
		resource := url.NewResource(descriptor.SchemaURL)
		if err := resource.Decode(&input); err != nil {
			return err
		}
	}
	if specification != nil && toolbox.AsString(specification) != "" {
		if err := toolbox.DefaultConverter.AssignConverted(&input, specification); err != nil {
			return err
		}
	}

	if table != "" {
		input.TableName = &table
	}
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()
	db, err := asDatabase(connection)
	if err != nil {
		return err
	}

	//TODO create only if table key is different, and drop all data instead for testing
	_, err = db.CreateTable(input)
	if err != nil {
		return err
	}

	waitForCreateCompletion(db, table)
	return err
}

func waitForCreateCompletion(db *dynamodb.DynamoDB, table string) {
	startTime := time.Now()
	for time.Now().Sub(startTime) < maxWaitTime {
		output, err := db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			break
		}
		if *output.Table.TableStatus != "CREATING" {
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (d *dialect) GetDatastores(manager dsc.Manager) ([]string, error) {
	config := manager.Config()
	return []string{config.Get(dbnameKey)}, nil
}

func (d *dialect) GetCurrentDatastore(manager dsc.Manager) (string, error) {
	config := manager.Config()
	return config.Get(dbnameKey), nil
}

func (d *dialect) GetTables(manager dsc.Manager, datastore string) ([]string, error) {
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	db, err := asDatabase(connection)
	if err != nil {
		return nil, err
	}
	output, err := db.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return nil, err
	}
	var result = make([]string, 0)
	if len(output.TableNames) == 0 {
		return result, nil
	}
	for _, table := range output.TableNames {
		result = append(result, *table)
	}
	return result, nil
}

func (d *dialect) CanCreateDatastore(manager dsc.Manager) bool {
	return false
}

func (d *dialect) CanDropDatastore(manager dsc.Manager) bool {
	return false
}

func (d *dialect) CanPersistBatch() bool {
	return false
}

func newDialect() dsc.DatastoreDialect {
	var resut dsc.DatastoreDialect = &dialect{dsc.NewDefaultDialect()}
	return resut
}
