package dyndb

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/viant/dsc"
	"github.com/viant/sqlparser"
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

// runDeleteAll - uses brute force scan and deletion one by one (testing only)
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
	if strings.HasPrefix(strings.TrimSpace(strings.ToLower(sql)), "create") {
		return m.createTableExecution(context.Background(), db, sql)
	} else if strings.HasPrefix(strings.TrimSpace(strings.ToLower(sql)), "drop") {
		return m.dropTableExecution(context.Background(), db, sql)
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

	sel, projection, mapped := normalizeExpr(statement)
	input := &dynamodb.ScanInput{
		TableName:                aws.String(statement.Table),
		ProjectionExpression:     projection,
		Select:                   sel,
		ExpressionAttributeNames: mapped,
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

	if sel != nil {
		return m.handleAggregation(db, input, statement, readingHandler)
	}

	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	for {
		output, err := db.Scan(input)
		if err != nil {
			return err
		}
		if len(statement.Columns) == 0 && len(output.Items) > 0 {
			for key := range output.Items[0] {
				statement.Columns = append(statement.Columns, &dsc.SQLColumn{Name: key})
			}
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

		lastEvaluatedKey = output.LastEvaluatedKey
		if lastEvaluatedKey == nil {
			return nil
		}
	}
}

func (m *manager) handleAggregation(db *dynamodb.DynamoDB, input *dynamodb.ScanInput, statement *dsc.QueryStatement, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	statement.Columns[0].Name = statement.Columns[0].Alias
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	var count int
	for {
		output, err := db.Scan(input)
		if err != nil {
			return err
		}
		lastEvaluatedKey = output.LastEvaluatedKey
		if output.Count != nil {
			count += int(*output.Count)
		}
		if lastEvaluatedKey == nil {
			scanner := dsc.NewSQLScanner(statement, m.Config(), nil)
			alias := statement.Columns[0].Alias
			if alias == "" {
				alias = statement.Columns[0].Expression
			}
			scanner.Values = map[string]interface{}{
				alias: count,
			}
			_, err = readingHandler(scanner)
			return err
		}
	}
	return nil
}

func normalizeExpr(statement *dsc.QueryStatement) (*string, *string, map[string]*string) {
	var result = make([]string, 0)
	var sel, proj *string
	var mapped map[string]*string
	columnNames := statement.ColumnNames()
	if len(statement.Columns) > 0 {
		if strings.HasPrefix(strings.ToLower(statement.Columns[0].Expression), "count") {
			sel = aws.String("COUNT")
		}
	}
	if sel == nil {
		for _, name := range columnNames {
			switch name {
			case "Date":
				name = "#Date"
				if len(mapped) == 0 {
					mapped = make(map[string]*string)
				}
				mapped["#Date"] = aws.String("Date")
			case "User":
				name = "#User"
				if len(mapped) == 0 {
					mapped = make(map[string]*string)
				}
				mapped["#User"] = aws.String("User")
			}
			result = append(result, name)
		}
	}
	if len(result) > 0 {
		proj = aws.String(strings.Join(result, ","))
	}
	return sel, proj, mapped
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

func (m *manager) createTableExecution(ctx context.Context, db *dynamodb.DynamoDB, SQL string) (sql.Result, error) {
	spec, err := sqlparser.ParseCreateTable(SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	tableName := sqlparser.TableName(spec)
	info := m.describeTable(db, tableName)
	if spec.IfDoesExists {
		if info != nil {
			return dsc.NewSQLResult(0, 0), nil
		}
	}

	capacityUnits := int64(1)
	input := &dynamodb.CreateTableInput{
		TableName:             &tableName,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{ReadCapacityUnits: &capacityUnits, WriteCapacityUnits: &capacityUnits},
	}
	for _, column := range spec.Columns {
		attrType, err := databaseAttributeType(column.Type)
		if err != nil {
			return nil, err
		}
		input.AttributeDefinitions = append(input.AttributeDefinitions, &dynamodb.AttributeDefinition{
			AttributeName: &column.Name,
			AttributeType: &attrType,
		})
		if key := column.Key; key != "" {
			key = strings.ToUpper(key)
			key = strings.TrimSpace(strings.Replace(key, "KEY", "", 1))
			input.KeySchema = append(input.KeySchema, &dynamodb.KeySchemaElement{
				AttributeName: &column.Name,
				KeyType:       &key,
			})
		}
	}

	if _, err = db.CreateTable(input); err != nil {
		return nil, err
	}
	waitForCreateCompletion(db, tableName)
	return dsc.NewSQLResult(0, 0), nil
}

func (m *manager) dropTableExecution(ctx context.Context, db *dynamodb.DynamoDB, SQL string) (sql.Result, error) {
	spec, err := sqlparser.ParseDropTable(SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	tableName := sqlparser.TableName(spec)
	info := m.describeTable(db, tableName)
	if spec.IfExists {
		if info == nil {
			return dsc.NewSQLResult(0, 0), nil
		}
	}

	if _, err = db.DeleteTable(&dynamodb.DeleteTableInput{TableName: &tableName}); err != nil {
		return nil, err
	}
	waitForTableDeletion(db, tableName)
	return dsc.NewSQLResult(0, 0), nil
}

func (m *manager) describeTable(db *dynamodb.DynamoDB, tableName string) *dynamodb.TableDescription {
	var result *dynamodb.TableDescription
	if output, _ := db.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}); output != nil {
		result = output.Table
	}
	return result
}
