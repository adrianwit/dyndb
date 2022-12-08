package dyndb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strings"
)

func getAttributeType(attributeValue *dynamodb.AttributeValue) string {
	if attributeValue.S != nil {
		return "S"
	}
	if attributeValue.N != nil {
		return "N"
	}

	if attributeValue.BOOL != nil {
		return "BOOL"
	}
	if attributeValue.B != nil {
		return "B"
	}
	return "S"
}

func databaseAttributeType(databaseType string) (string, error) {
	switch strings.ToLower(databaseType) {
	case "int", "numeric", "decimal":
		return "N", nil
	case "bool":
		return "BOOL", nil
	case "varchar", "text", "string":
		return "S", nil
	}
	return "", fmt.Errorf("unsupported key type: %v", databaseType)
}
