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
	Type      string  `json:"type"`
	Analyzer  string  `json:"analyzer"`
	Boost     float64 `json:"boost"`
	Fuzziness int     `json:"fuzziness"`
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
	case "match_phrase":
		fallthrough
	case "match":
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
		output["analyzer"] = opt.Analyzer
		output["boost"] = opt.Boost
		if opt.Fuzziness > 0 {
			output["fuzziness"] = opt.Fuzziness
		}
		outputJSON, err := json.Marshal(output)
		if err != nil {
			return nil, fmt.Errorf("err: %v", err)
		}
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
		input   string
		options string
	}{
		{
			field:   "title",
			input:   "+Avengers~2 +company:marvel",
			options: "",
		},
		{
			field:   "title",
			input:   "+avengers thor",
			options: "{}",
		},
		{
			field:   "title",
			input:   "avengers",
			options: `{"type": "match", "fuzziness": 2}`,
		},
		{
			field:   "title",
			input:   "Avengers: Infinity War",
			options: `{"type": "match_phrase", "analyzer": "en", "boost": 10}`,
		},
		{
			field:   "title",
			input:   "Avengers*",
			options: `{"type": "wildcard", "analyzer": "en"}`,
		},
	}

	for i, test := range tests {
		q, err := PrepQuery(test.field, test.input, test.options)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%d: %#v\n", i+1, q)
	}
}
