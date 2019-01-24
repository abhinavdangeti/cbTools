package main

import (
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/search/query"
)

func updateFieldsInQuery(q query.Query, field string) error {
	switch q.(type) {
	case *query.BooleanQuery:
		updateFieldsInQuery(q.(*query.BooleanQuery).Must, field)
		updateFieldsInQuery(q.(*query.BooleanQuery).Should, field)
		updateFieldsInQuery(q.(*query.BooleanQuery).MustNot, field)
	case *query.ConjunctionQuery:
		cq := q.(*query.ConjunctionQuery)
		for i := 0; i < len(cq.Conjuncts); i++ {
			updateFieldsInQuery(cq.Conjuncts[i], field)
		}
	case *query.DisjunctionQuery:
		dq := q.(*query.DisjunctionQuery)
		for i := 0; i < len(dq.Disjuncts); i++ {
			updateFieldsInQuery(dq.Disjuncts[i], field)
		}
	default:
		if fq, ok := q.(query.FieldableQuery); ok {
			if fq.Field() == "" && field != "" {
				fq.SetField(field)
			}
		}
	}

	return nil
}

type Options struct {
	Type         string  `json:"type"`
	Analyzer     string  `json:"analyzer"`
	Boost        float64 `json:"boost"`
	Fuzziness    int     `json:"fuzziness"`
	PrefixLength int     `json:"prefix_length"`
	Operator     string  `json:"operator"`
}

func PrepQuery(field, input, options string) (query.Query, error) {
	opt := Options{}
	if options != "" {
		err := json.Unmarshal([]byte(options), &opt)
		if err != nil {
			return nil, fmt.Errorf("err: %v", err)
		}
	}

	switch opt.Type {
	case "query_string++":
		fallthrough
	case "search":
		fallthrough
	case "":
		fallthrough
	case "query_string":
		fallthrough
	case "query":
		qsq := query.NewQueryStringQuery(input)
		q, err := qsq.Parse()
		if err != nil {
			return nil, fmt.Errorf("qsq.Parse, err: %v", err)
		}

		err = updateFieldsInQuery(q, field)
		if err != nil {
			return nil, fmt.Errorf("updateFieldsInQuery, err: %v", err)
		}
		return q, nil
	case "bool":
		fallthrough
	case "match_phrase":
		fallthrough
	case "match":
		fallthrough
	case "prefix":
		fallthrough
	case "regexp":
		fallthrough
	case "wildcard":
		fallthrough
	case "terms":
		fallthrough
	case "term":
		output := map[string]interface{}{}
		output["field"] = field
		output[opt.Type] = input
		if opt.Analyzer != "" {
			output["analyzer"] = opt.Analyzer
		}
		output["boost"] = opt.Boost
		if opt.Fuzziness > 0 {
			output["fuzziness"] = opt.Fuzziness
		}
		if opt.PrefixLength > 0 {
			output["prefix_length"] = opt.PrefixLength
		}
		if opt.Operator != "" {
			output["operator"] = opt.Operator
		}
		outputJSON, err := json.Marshal(output)
		if err != nil {
			return nil, fmt.Errorf("err: %v", err)
		}
		fmt.Println(string(outputJSON))
		q, err := query.ParseQuery(outputJSON)
		if err != nil {
			return nil, fmt.Errorf("ParseQuery, err: %v", err)
		}
		return q, nil
	default:
		return nil, fmt.Errorf("not supported: %v", opt.Type)
	}
}

func main() {
	tests := []struct {
		field   string
		query   string
		options string
	}{
		{
			field:   "title",
			query:   "+Avengers~2 +company:marvel",
			options: "",
		},
		{
			field:   "title",
			query:   "+avengers thor",
			options: "{}",
		},
		{
			field:   "title",
			query:   "avengers",
			options: `{"type": "match", "fuzziness": 2}`,
		},
		{
			field:   "title",
			query:   "Avengers: Infinity War",
			options: `{"type": "match_phrase", "analyzer": "en", "boost": 10}`,
		},
		{
			field:   "title",
			query:   "Avengers*",
			options: `{"type": "wildcard", "analyzer": "en"}`,
		},
	}

	for i, test := range tests {
		q, err := PrepQuery(test.field, test.query, test.options)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%d: %#v\n", i+1, q)
	}
}
