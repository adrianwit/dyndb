package dyndb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/cred"
	"github.com/viant/toolbox/secret"
	"strings"
)

const (
	keyKey      = "key"
	secretKey   = "secret"
	regionKey   = "region"
	endpointKey = "endpoint"

	dbnameKey = "dbname"
)

var DbPointer = (*dynamodb.DynamoDB)(nil)

func asDatabase(connection dsc.Connection) (*dynamodb.DynamoDB, error) {
	db := connection.Unwrap(DbPointer).(*dynamodb.DynamoDB)
	return db, nil
}

type connection struct {
	*dsc.AbstractConnection
	db *dynamodb.DynamoDB
}

func (c *connection) CloseNow() error {
	return nil
}

func (c *connection) Unwrap(targetType interface{}) interface{} {
	if targetType == DbPointer {
		return c.db
	}
	panic(fmt.Sprintf("unsupported targetType type %v", targetType))
}

type connectionProvider struct {
	*dsc.AbstractConnectionProvider
}

func (p *connectionProvider) NewConnection() (dsc.Connection, error) {
	config := p.ConnectionProvider.Config()
	credConfig, err := getCredConfig(config)
	if err != nil {
		return nil, err
	}
	awsConfig := getAWSConfig(credConfig)
	if awsConfig.Region == nil {
		return nil, fmt.Errorf("region was empty")
	}
	awsConfig = p.applyOptions(awsConfig)
	sess := session.Must(session.NewSession())
	db := dynamodb.New(sess, awsConfig)
	var connection = &connection{db: db}
	var super = dsc.NewAbstractConnection(config, p.ConnectionProvider.ConnectionPool(), connection)
	connection.AbstractConnection = super
	return connection, nil
}

func (p *connectionProvider) applyOptions(awsConfig *aws.Config) *aws.Config {
	p.updateParameters()
	if p.Config().Has(endpointKey) {
		endpoint := p.Config().Get(endpointKey)
		if !strings.Contains(endpoint, ":") {
			endpoint = endpoint + ":8000"
		}
		if !strings.Contains(endpoint, "http") {
			endpoint = "http://" + endpoint
		}
		awsConfig = awsConfig.WithEndpoint(endpoint)
	}

	if p.Config().Has(regionKey) {
		region := p.Config().Get(regionKey)
		awsConfig = awsConfig.WithRegion(region)
	}
	if p.Config().Has(keyKey) {
		key := p.Config().Get(keyKey)
		secret := p.Config().Get(secretKey)
		c := credentials.NewStaticCredentials(key, secret, "")
		awsConfig = awsConfig.WithCredentials(c)
	}
	return awsConfig
}

func (p *connectionProvider) updateParameters() {
	paramMap := toolbox.MakeMap(p.Config().Descriptor, ":", ",")
	for k, v := range paramMap {
		if _, ok := p.Config().Parameters[k]; ok {
			continue
		}
		p.Config().Parameters[k] = v
	}
}

func newConnectionProvider(config *dsc.Config) dsc.ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	aerospikeConnectionProvider := &connectionProvider{}
	var connectionProvider dsc.ConnectionProvider = aerospikeConnectionProvider
	var super = dsc.NewAbstractConnectionProvider(config, make(chan dsc.Connection, config.MaxPoolSize), connectionProvider)
	aerospikeConnectionProvider.AbstractConnectionProvider = super
	aerospikeConnectionProvider.AbstractConnectionProvider.ConnectionProvider = connectionProvider
	return aerospikeConnectionProvider
}

func getCredConfig(config *dsc.Config) (*cred.Config, error) {
	var err error
	credConfig := &cred.Config{}
	if config.Credentials != "" {
		credConfig, err = secret.New("", false).GetCredentials(config.Credentials)
		if err != nil {
			return nil, err
		}
	}
	if config.Has(keyKey) {
		credConfig.Key = config.Get(keyKey)
	}
	if config.Has(secretKey) {
		credConfig.Secret = config.Get(secretKey)
	}
	if config.Has(regionKey) {
		credConfig.Region = config.Get(regionKey)
	}
	return credConfig, nil
}

//getAWSConfig returns *aws.Config for provided credential
func getAWSConfig(credConfig *cred.Config) *aws.Config {
	result := &aws.Config{}
	if credConfig.Key != "" {
		awsCredentials := credentials.NewStaticCredentials(credConfig.Key, credConfig.Secret, "")
		result = aws.NewConfig().WithRegion(credConfig.Region).WithCredentials(awsCredentials)

	} else if credConfig.Region != "" {
		result.Region = &credConfig.Region
	}
	return result
}
