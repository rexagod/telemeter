package metricfamily

import (
	clientmodel "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type whitelist [][]*labels.Matcher

// NewWhitelist returns a Transformer that checks if at least one
// rule in the whitelist is true.
// This Transformer will nil metrics within a metric family that do not match a rule.
// Each given rule is transformed into a matchset. Matchsets are OR-ed.
// Individual matchers within a matchset are AND-ed, as in PromQL.
func NewWhitelist(rules []string) (Transformer, error) {
	var ms [][]*labels.Matcher
	for _, rule := range rules {
		expr, err := parser.ParseExpr(rule)
		if err != nil {
			return nil, err
		}
		parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
			if node, ok := node.(*parser.VectorSelector); ok {
				ms = append(ms, node.LabelMatchers)
			}
			return nil
		})
	}
	return whitelist(ms), nil
}

// Transform implements the Transformer interface.
func (t whitelist) Transform(family *clientmodel.MetricFamily) (bool, error) {
	var ok bool
Metric:
	for i, m := range family.Metric {
		if m == nil {
			continue
		}
		for _, matchset := range t {
			if match(family.GetName(), m, matchset...) {
				ok = true
				continue Metric
			}
		}
		family.Metric[i] = nil
	}
	return ok, nil
}

// match checks whether every Matcher matches a given metric.
func match(name string, metric *clientmodel.Metric, matchers ...*labels.Matcher) bool {
Matcher:
	for _, m := range matchers {
		if m.Name == "__name__" && m.Matches(name) {
			continue
		}
		for _, label := range metric.Label {
			if label == nil || m.Name != label.GetName() || !m.Matches(label.GetValue()) {
				continue
			}
			continue Matcher
		}
		return false
	}
	return true
}
