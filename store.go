package dynamo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/zerops/eventsource"
)

const (
	DefaultRegion   = "us-east-1"
	DefaultHashKey  = "key"
	DefaultRangeKey = "partition"
)

const (
	// prefix prefixes the event keys in the dynamodb item
	prefix = "_"

	// atBase refers to the base encoding for the record at
	atBase = 36
)

var (
	errInvalidKey = errors.New("invalid event key")
	errNoRecords  = errors.New("no records to save")
)

func IsKey(key string) bool {
	return strings.HasPrefix(key, prefix)
}

// Store represents a dynamodb backed eventsource.Store
type Store struct {
	region        string
	tableName     string
	hashKey       string
	rangeKey      string
	api           *dynamodb.DynamoDB
	useStreams    bool
	eventsPerItem int
	debug         bool
	writer        io.Writer
}

// Save implements the eventsource.Store interface
func (s *Store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	if len(records) == 0 {
		return nil
	}

	input, err := makeUpdateItemInput(s.tableName, s.hashKey, s.rangeKey, s.eventsPerItem, aggregateID, records...)
	if err != nil {
		return err
	}

	if s.debug {
		encoder := json.NewEncoder(s.writer)
		encoder.SetIndent("", "  ")
		encoder.Encode(input)
	}

	_, err = s.api.UpdateItem(input)
	if err != nil {
		if v, ok := err.(awserr.Error); ok {
			return errors.Wrapf(err, "Save failed. %v [%v]", v.Message(), v.Code())
		}
		return err
	}

	return nil
}

func (s *Store) logf(format string, args ...interface{}) {
	if s.debug {
		return
	}

	io.WriteString(s.writer, time.Now().Format(time.StampMilli))
	io.WriteString(s.writer, " ")
	fmt.Fprintf(s.writer, format, args...)

	if !strings.HasSuffix(format, "\n") {
		io.WriteString(s.writer, "\n")
	}
}

func (s *Store) Fetch(ctx context.Context, aggregateID string, version int) (eventsource.History, error) {
	partition := selectPartition(version, s.eventsPerItem)
	input, err := makeQueryInput(s.tableName, s.hashKey, s.rangeKey, aggregateID, partition)
	if err != nil {
		return eventsource.History{}, err
	}

	history := make(eventsource.History, 0, version)

	var startKey map[string]*dynamodb.AttributeValue
	for {
		out, err := s.api.Query(input)
		if err != nil {
			return eventsource.History{}, err
		}

		if len(out.Items) == 0 {
			break
		}

		// events are stored within av as _t{version} = {event-type}, _d{version} = {serialized event}
		for _, item := range out.Items {
			for key, av := range item {
				if !IsKey(key) {
					continue
				}

				recordVersion, recordAt, err := VersionAndAt(key)
				if err != nil {
					return nil, err
				}

				if version > 0 && recordVersion > version {
					continue
				}

				history = append(history, eventsource.Record{
					Version: recordVersion,
					At:      recordAt,
					Data:    av.B,
				})
			}
		}

		startKey = out.LastEvaluatedKey
		if len(startKey) == 0 {
			break
		}
	}

	sort.Slice(history, func(i, j int) bool {
		return history[i].Version < history[j].Version
	})

	return history, nil
}

func New(tableName string, opts ...Option) (*Store, error) {
	store := &Store{
		region:        DefaultRegion,
		tableName:     tableName,
		hashKey:       DefaultHashKey,
		rangeKey:      DefaultRangeKey,
		eventsPerItem: 1,
	}

	for _, opt := range opts {
		opt(store)
	}

	if store.api == nil {
		cfg := &aws.Config{Region: aws.String(store.region)}
		s, err := session.NewSession(cfg)
		if err != nil {
			if v, ok := err.(awserr.Error); ok {
				return nil, errors.Wrapf(err, "Unable to create AWS Session - %v [%v]", v.Message(), v.Code())
			}
			return nil, err
		}
		store.api = dynamodb.New(s)
	}

	return store, nil
}

func makeUpdateItemInput(tableName, hashKey, rangeKey string, eventsPerItem int, aggregateID string, records ...eventsource.Record) (*dynamodb.UpdateItemInput, error) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Version < records[j].Version
	})

	if len(records) == 0 {
		return nil, errNoRecords
	}

	partitionID := records[0].Version / eventsPerItem

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			hashKey:  {S: aws.String(aggregateID)},
			rangeKey: {N: aws.String(strconv.Itoa(partitionID))},
		},
		ExpressionAttributeNames: map[string]*string{
			"#revision": aws.String("revision"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {N: aws.String("1")},
		},
	}

	// Add each element within the partition to the UpdateItemInput

	condExpr := &bytes.Buffer{}
	updateExpr := &bytes.Buffer{}
	io.WriteString(updateExpr, "ADD #revision :one SET ")

	for index, record := range records {
		version := strconv.Itoa(record.Version)
		at := strconv.FormatInt(record.At.Int64(), atBase)

		// Each event is store as two entries, an event entries and an event type entry.

		key := prefix + version + ":" + at
		nameRef := "#" + prefix + version
		valueRef := ":" + prefix + version

		if index > 0 {
			io.WriteString(condExpr, " AND ")
			io.WriteString(updateExpr, ", ")
		}
		fmt.Fprintf(condExpr, "attribute_not_exists(%v)", nameRef)
		fmt.Fprintf(updateExpr, "%v = %v", nameRef, valueRef)
		input.ExpressionAttributeNames[nameRef] = aws.String(key)
		input.ExpressionAttributeValues[valueRef] = &dynamodb.AttributeValue{B: record.Data}
	}

	input.ConditionExpression = aws.String(condExpr.String())
	input.UpdateExpression = aws.String(updateExpr.String())

	return input, nil
}

// makeQueryInput
//  - partition - fetch up to this partition number; 0 to fetch all partitions
func makeQueryInput(tableName, hashKey, rangeKey string, aggregateID string, partition int) (*dynamodb.QueryInput, error) {
	input := &dynamodb.QueryInput{
		TableName:      aws.String(tableName),
		Select:         aws.String("ALL_ATTRIBUTES"),
		ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]*string{
			"#key": aws.String(hashKey),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":key": {S: aws.String(aggregateID)},
		},
	}

	if partition == 0 {
		input.KeyConditionExpression = aws.String("#key = :key")

	} else {
		input.KeyConditionExpression = aws.String("#key = :key AND #partition <= :partition")
		input.ExpressionAttributeNames["#partition"] = aws.String(rangeKey)
		input.ExpressionAttributeValues[":partition"] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(partition))}
	}

	return input, nil
}

func selectPartition(version, eventsPerItem int) int {
	return version / eventsPerItem
}

func VersionAndAt(key string) (int, eventsource.EpochMillis, error) {
	if !strings.HasPrefix(key, prefix) {
		return 0, 0, errInvalidKey
	}

	segments := strings.Split(key[1:], ":")
	if len(segments) != 2 {
		return 0, 0, errInvalidKey
	}

	version, err := strconv.Atoi(segments[0])
	if err != nil {
		return 0, 0, errInvalidKey
	}

	millis, err := strconv.ParseInt(segments[1], atBase, 64)
	if err != nil {
		return 0, 0, errInvalidKey
	}

	return version, eventsource.EpochMillis(millis), nil
}
