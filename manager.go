package dyndb

import (
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
)

type manager struct {
	*dsc.AbstractManager
}

func (m *manager) runInsert(db *dynamodb.DynamoDB, statement *dsc.DmlStatement, sqlParameters []interface{}) (err error) {
	parameters := toolbox.NewSliceIterator(sqlParameters)
	var record map[string]interface{}
	if record, err = statement.ColumnValueMap(parameters); err != nil {
		return err
	}
	attributeValues, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		Item:      attributeValues,
		TableName: aws.String(statement.Table),
	}
	_, err = db.PutItem(input)
	return err
}

func (m *manager) runUpdate(db *dynamodb.DynamoDB, statement *dsc.DmlStatement, sqlParameters []interface{}) (err error) {
	parameters := toolbox.NewSliceIterator(sqlParameters)
	var record map[string]interface{}
	if record, err = statement.ColumnValueMap(parameters); err != nil {
		return err
	}
	if len(record) == 0 { //nothing to change
		return nil
	}
	keyValues, err := getKeyCriteriaMap(statement.SQLCriteria, parameters)
	if err != nil {
		return err
	}
	if len(keyValues) == 0 {
		return fmt.Errorf("getCriteriaExpression was empty")
	}
	if statement.Criteria[0].Operator != "=" {
		return fmt.Errorf("unsupported getCriteriaExpression operator %v", statement.SQLCriteria.Expression())
	}
	updateAttributes, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return err
	}
	keyAttributes, err := dynamodbattribute.MarshalMap(keyValues)
	if err != nil {
		return err
	}

	input := &dynamodb.UpdateItemInput{
		AttributeUpdates: make(map[string]*dynamodb.AttributeValueUpdate),
		TableName:        aws.String(statement.Table),
		Key:              keyAttributes,
	}
	for k, v := range updateAttributes {
		input.AttributeUpdates[k] = &dynamodb.AttributeValueUpdate{
			Action: aws.String("PUT"),
			Value:  v,
		}
	}
	_, err = db.UpdateItem(input)
	return err
}

func (m *manager) runDelete(db *dynamodb.DynamoDB, statement *dsc.DmlStatement, sqlParameters []interface{}) (affected int, err error) {
	parameters := toolbox.NewSliceIterator(sqlParameters)
	keyValues, err := getKeyCriteriaMap(statement.SQLCriteria, parameters)
	if err != nil {
		return 0, err
	}
	if len(keyValues) == 0 {
		return m.runDeleteAll(db, statement, sqlParameters)
	}
	if statement.Criteria[0].Operator != "=" {
		return 0, fmt.Errorf("unsupported getCriteriaExpression operator %v", statement.SQLCriteria.Expression())
	}
	keyAttributes, err := dynamodbattribute.MarshalMap(keyValues)
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(statement.Table),
		Key:       keyAttributes,
	}
	_, err = db.DeleteItem(input)
	return 1, err
}

//runDeleteAll - uses brute force scan and deletion one by one (testing only)
func (m *manager) runDeleteAll(db *dynamodb.DynamoDB, statement *dsc.DmlStatement, sqlParameters []interface{}) (affected int, err error) {
	output, err := db.Scan(&dynamodb.ScanInput{
		TableName: aws.String(statement.Table),
	})
	if err != nil {
		return 0, err
	}
	dynamoDbDialect := &dialect{}
	keys, err := dynamoDbDialect.getKeyAttributes(m, statement.Table)
	if err != nil {
		return 0, err
	}
	affected = 0
	for _, item := range output.Items {
		keyAttributes := make(map[string]*dynamodb.AttributeValue)
		for _, key := range keys {
			keyAttributes[*key.AttributeName] = item[*key.AttributeName]
		}
		if _, err = db.DeleteItem(&dynamodb.DeleteItemInput{
			Key:       keyAttributes,
			TableName: aws.String(statement.Table),
		}); err != nil {
			return 0, err
		}
		affected++

	}
	return affected, nil
}

func (m *manager) ExecuteOnConnection(connection dsc.Connection, sql string, sqlParameters []interface{}) (result sql.Result, err error) {
	dsc.Logf("[dynampDB]:%v, %v\n", sql, sqlParameters)
	db, err := asDatabase(connection)
	if err != nil {
		return nil, err
	}
	parser := dsc.NewDmlParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %v due to %v", sql, err)
	}
	var affectedRecords = 1
	switch statement.Type {
	case "INSERT":
		err = m.runInsert(db, statement, sqlParameters)
	case "UPDATE":
		err = m.runUpdate(db, statement, sqlParameters)
	case "DELETE":
		affectedRecords, err = m.runDelete(db, statement, sqlParameters)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to modify %v, %v", statement.Table, err)
	}
	return dsc.NewSQLResult(int64(affectedRecords), 0), nil
}

func (m *manager) ReadAllOnWithHandlerOnConnection(connection dsc.Connection, SQL string, sqlParameters []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	dsc.Logf("[dynamoDB]:%v, %v\n", SQL, sqlParameters)
	db, err := asDatabase(connection)
	if err != nil {
		return err
	}
	parser := dsc.NewQueryParser()
	statement, err := parser.Parse(SQL)
	if err != nil {
		return fmt.Errorf("failed to parse statement %v, %v", SQL, err)
	}

	if statement.SQLCriteria != nil && len(statement.Criteria) > 0 {
		ok, err := m.tryReadItem(db, statement, sqlParameters, readingHandler)
		if ok {
			return err
		}
	}
	projection := strings.Join(statement.ColumnNames(), ", ")
	input := &dynamodb.ScanInput{
		TableName:            aws.String(statement.Table),
		ProjectionExpression: aws.String(projection),
	}
	parameters := toolbox.NewSliceIterator(sqlParameters)
	criteria, criteriaAttributes, err := getCriteriaExpression(statement.SQLCriteria, parameters)
	if err != nil {
		return err
	}
	if len(criteriaAttributes) > 0 {
		input.FilterExpression = criteria
		input.ExpressionAttributeValues = criteriaAttributes
	}
	output, err := db.Scan(input)
	if err != nil {
		return err
	}
	if len(output.Items) == 0 {
		return nil
	}
	for _, item := range output.Items {
		scanner := dsc.NewSQLScanner(statement, m.Config(), nil)
		scanner.Values = make(map[string]interface{})
		if err := dynamodbattribute.UnmarshalMap(item, &scanner.Values); err != nil {
			return err
		}
		toContinue, err := readingHandler(scanner)
		if err != nil {
			return err
		}
		if !toContinue {
			break
		}
	}
	return nil
}

func (m *manager) tryReadItem(db *dynamodb.DynamoDB, statement *dsc.QueryStatement, parameters []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) (bool, error) {
	valueMap, err := getKeyCriteriaMap(statement.SQLCriteria, toolbox.NewSliceIterator(parameters))
	if err != nil {
		return false, err
	}
	dynamoDbDialect := &dialect{}
	var indexedKeys = make(map[string]bool)
	for _, key := range strings.Split(dynamoDbDialect.GetKeyName(m, "", statement.Table), ",") {
		indexedKeys[key] = true
	}
	toContinue := true
	return processCriteria(valueMap, func(keyValues map[string]interface{}) (bool, error) {
		if !toContinue {
			return true, nil
		}
		if len(indexedKeys) != len(keyValues) {
			return false, nil
		}
		for criteriaKey := range keyValues {
			if !indexedKeys[criteriaKey] {
				return false, nil
			}
		}
		keyAttributes, err := dynamodbattribute.MarshalMap(keyValues)
		if err != nil {
			return false, err
		}
		output, err := db.GetItem(&dynamodb.GetItemInput{
			TableName: aws.String(statement.Table),
			Key:       keyAttributes,
		})
		if output.Item == nil {
			return true, nil
		}
		scanner := dsc.NewSQLScanner(statement, m.Config(), nil)
		scanner.Values = make(map[string]interface{})
		if err := dynamodbattribute.UnmarshalMap(output.Item, &scanner.Values); err != nil {
			return true, err
		}
		toContinue, err = readingHandler(scanner)
		if err != nil {
			return true, err
		}
		return true, nil
	})
}
