package nqm_parser

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"
)

var g = &grammar{
	rules: []*rule{
		{
			name: "Query",
			pos:  position{line: 5, col: 1, offset: 24},
			expr: &actionExpr{
				pos: position{line: 5, col: 9, offset: 32},
				run: (*parser).callonQuery1,
				expr: &seqExpr{
					pos: position{line: 5, col: 9, offset: 32},
					exprs: []interface{}{
						&labeledExpr{
							pos:   position{line: 5, col: 9, offset: 32},
							label: "allParams",
							expr: &zeroOrMoreExpr{
								pos: position{line: 5, col: 19, offset: 42},
								expr: &ruleRefExpr{
									pos:  position{line: 5, col: 19, offset: 42},
									name: "QueryParam",
								},
							},
						},
						&ruleRefExpr{
							pos:  position{line: 5, col: 31, offset: 54},
							name: "EOF",
						},
					},
				},
			},
		},
		{
			name: "QueryParam",
			pos:  position{line: 17, col: 1, offset: 303},
			expr: &choiceExpr{
				pos: position{line: 17, col: 14, offset: 316},
				alternatives: []interface{}{
					&actionExpr{
						pos: position{line: 17, col: 14, offset: 316},
						run: (*parser).callonQueryParam2,
						expr: &seqExpr{
							pos: position{line: 17, col: 14, offset: 316},
							exprs: []interface{}{
								&ruleRefExpr{
									pos:  position{line: 17, col: 14, offset: 316},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 17, col: 16, offset: 318},
									label: "param",
									expr: &choiceExpr{
										pos: position{line: 17, col: 23, offset: 325},
										alternatives: []interface{}{
											&ruleRefExpr{
												pos:  position{line: 17, col: 23, offset: 325},
												name: "TimeFilter",
											},
											&ruleRefExpr{
												pos:  position{line: 17, col: 36, offset: 338},
												name: "NodeFilter",
											},
										},
									},
								},
								&ruleRefExpr{
									pos:  position{line: 17, col: 48, offset: 350},
									name: "_",
								},
							},
						},
					},
					&actionExpr{
						pos: position{line: 19, col: 5, offset: 377},
						run: (*parser).callonQueryParam10,
						expr: &seqExpr{
							pos: position{line: 19, col: 5, offset: 377},
							exprs: []interface{}{
								&labeledExpr{
									pos:   position{line: 19, col: 5, offset: 377},
									label: "paramName",
									expr: &ruleRefExpr{
										pos:  position{line: 19, col: 15, offset: 387},
										name: "ParamName",
									},
								},
								&zeroOrOneExpr{
									pos: position{line: 19, col: 25, offset: 397},
									expr: &seqExpr{
										pos: position{line: 19, col: 26, offset: 398},
										exprs: []interface{}{
											&litMatcher{
												pos:        position{line: 19, col: 26, offset: 398},
												val:        "=",
												ignoreCase: false,
											},
											&zeroOrOneExpr{
												pos: position{line: 19, col: 30, offset: 402},
												expr: &ruleRefExpr{
													pos:  position{line: 19, col: 30, offset: 402},
													name: "ParamValue",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "NodeFilter",
			pos:  position{line: 23, col: 1, offset: 496},
			expr: &choiceExpr{
				pos: position{line: 23, col: 14, offset: 509},
				alternatives: []interface{}{
					&actionExpr{
						pos: position{line: 23, col: 14, offset: 509},
						run: (*parser).callonNodeFilter2,
						expr: &seqExpr{
							pos: position{line: 23, col: 14, offset: 509},
							exprs: []interface{}{
								&labeledExpr{
									pos:   position{line: 23, col: 14, offset: 509},
									label: "nodeProperty",
									expr: &ruleRefExpr{
										pos:  position{line: 23, col: 27, offset: 522},
										name: "NodeProperty",
									},
								},
								&litMatcher{
									pos:        position{line: 23, col: 40, offset: 535},
									val:        "=",
									ignoreCase: false,
								},
								&labeledExpr{
									pos:   position{line: 23, col: 44, offset: 539},
									label: "stringValues",
									expr: &ruleRefExpr{
										pos:  position{line: 23, col: 57, offset: 552},
										name: "MultiLiteralString",
									},
								},
							},
						},
					},
					&actionExpr{
						pos: position{line: 25, col: 5, offset: 636},
						run: (*parser).callonNodeFilter9,
						expr: &seqExpr{
							pos: position{line: 25, col: 5, offset: 636},
							exprs: []interface{}{
								&labeledExpr{
									pos:   position{line: 25, col: 5, offset: 636},
									label: "paramName",
									expr: &ruleRefExpr{
										pos:  position{line: 25, col: 15, offset: 646},
										name: "NodeProperty",
									},
								},
								&labeledExpr{
									pos:   position{line: 25, col: 28, offset: 659},
									label: "assignedValue",
									expr: &zeroOrOneExpr{
										pos: position{line: 25, col: 42, offset: 673},
										expr: &seqExpr{
											pos: position{line: 25, col: 43, offset: 674},
											exprs: []interface{}{
												&litMatcher{
													pos:        position{line: 25, col: 43, offset: 674},
													val:        "=",
													ignoreCase: false,
												},
												&ruleRefExpr{
													pos:  position{line: 25, col: 47, offset: 678},
													name: "ParamValue",
												},
											},
										},
									},
								},
								&ruleRefExpr{
									pos:  position{line: 25, col: 60, offset: 691},
									name: "END_WORD",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "TimeFilter",
			pos:  position{line: 29, col: 1, offset: 758},
			expr: &choiceExpr{
				pos: position{line: 29, col: 14, offset: 771},
				alternatives: []interface{}{
					&actionExpr{
						pos: position{line: 29, col: 14, offset: 771},
						run: (*parser).callonTimeFilter2,
						expr: &seqExpr{
							pos: position{line: 29, col: 14, offset: 771},
							exprs: []interface{}{
								&labeledExpr{
									pos:   position{line: 29, col: 14, offset: 771},
									label: "timeParamName",
									expr: &ruleRefExpr{
										pos:  position{line: 29, col: 28, offset: 785},
										name: "TimeParamName",
									},
								},
								&litMatcher{
									pos:        position{line: 29, col: 42, offset: 799},
									val:        "=",
									ignoreCase: false,
								},
								&labeledExpr{
									pos:   position{line: 29, col: 46, offset: 803},
									label: "timeValue",
									expr: &choiceExpr{
										pos: position{line: 29, col: 57, offset: 814},
										alternatives: []interface{}{
											&ruleRefExpr{
												pos:  position{line: 29, col: 57, offset: 814},
												name: "ISO_8601",
											},
											&ruleRefExpr{
												pos:  position{line: 29, col: 68, offset: 825},
												name: "UNIX_TIME",
											},
										},
									},
								},
							},
						},
					},
					&actionExpr{
						pos: position{line: 31, col: 5, offset: 899},
						run: (*parser).callonTimeFilter11,
						expr: &seqExpr{
							pos: position{line: 31, col: 5, offset: 899},
							exprs: []interface{}{
								&labeledExpr{
									pos:   position{line: 31, col: 5, offset: 899},
									label: "paramName",
									expr: &ruleRefExpr{
										pos:  position{line: 31, col: 15, offset: 909},
										name: "TimeParamName",
									},
								},
								&labeledExpr{
									pos:   position{line: 31, col: 29, offset: 923},
									label: "assignedValue",
									expr: &zeroOrOneExpr{
										pos: position{line: 31, col: 43, offset: 937},
										expr: &seqExpr{
											pos: position{line: 31, col: 44, offset: 938},
											exprs: []interface{}{
												&litMatcher{
													pos:        position{line: 31, col: 44, offset: 938},
													val:        "=",
													ignoreCase: false,
												},
												&zeroOrOneExpr{
													pos: position{line: 31, col: 48, offset: 942},
													expr: &ruleRefExpr{
														pos:  position{line: 31, col: 48, offset: 942},
														name: "ParamValue",
													},
												},
											},
										},
									},
								},
								&ruleRefExpr{
									pos:  position{line: 31, col: 62, offset: 956},
									name: "END_WORD",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "NodeProperty",
			pos:  position{line: 35, col: 1, offset: 1023},
			expr: &actionExpr{
				pos: position{line: 35, col: 16, offset: 1038},
				run: (*parser).callonNodeProperty1,
				expr: &seqExpr{
					pos: position{line: 35, col: 16, offset: 1038},
					exprs: []interface{}{
						&choiceExpr{
							pos: position{line: 35, col: 17, offset: 1039},
							alternatives: []interface{}{
								&litMatcher{
									pos:        position{line: 35, col: 17, offset: 1039},
									val:        "agent",
									ignoreCase: false,
								},
								&litMatcher{
									pos:        position{line: 35, col: 27, offset: 1049},
									val:        "target",
									ignoreCase: false,
								},
							},
						},
						&litMatcher{
							pos:        position{line: 35, col: 37, offset: 1059},
							val:        ".",
							ignoreCase: false,
						},
						&choiceExpr{
							pos: position{line: 35, col: 42, offset: 1064},
							alternatives: []interface{}{
								&litMatcher{
									pos:        position{line: 35, col: 42, offset: 1064},
									val:        "isp",
									ignoreCase: false,
								},
								&litMatcher{
									pos:        position{line: 35, col: 50, offset: 1072},
									val:        "province",
									ignoreCase: false,
								},
								&litMatcher{
									pos:        position{line: 35, col: 63, offset: 1085},
									val:        "city",
									ignoreCase: false,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "TimeParamName",
			pos:  position{line: 39, col: 1, offset: 1126},
			expr: &actionExpr{
				pos: position{line: 39, col: 17, offset: 1142},
				run: (*parser).callonTimeParamName1,
				expr: &choiceExpr{
					pos: position{line: 39, col: 18, offset: 1143},
					alternatives: []interface{}{
						&litMatcher{
							pos:        position{line: 39, col: 18, offset: 1143},
							val:        "starttime",
							ignoreCase: false,
						},
						&litMatcher{
							pos:        position{line: 39, col: 32, offset: 1157},
							val:        "endtime",
							ignoreCase: false,
						},
					},
				},
			},
		},
		{
			name: "ParamName",
			pos:  position{line: 43, col: 1, offset: 1201},
			expr: &actionExpr{
				pos: position{line: 43, col: 13, offset: 1213},
				run: (*parser).callonParamName1,
				expr: &oneOrMoreExpr{
					pos: position{line: 43, col: 13, offset: 1213},
					expr: &charClassMatcher{
						pos:        position{line: 43, col: 13, offset: 1213},
						val:        "[^ =\\t\\n\\r]",
						chars:      []rune{' ', '=', '\t', '\n', '\r'},
						ignoreCase: false,
						inverted:   true,
					},
				},
			},
		},
		{
			name: "ParamValue",
			pos:  position{line: 46, col: 1, offset: 1258},
			expr: &actionExpr{
				pos: position{line: 46, col: 14, offset: 1271},
				run: (*parser).callonParamValue1,
				expr: &oneOrMoreExpr{
					pos: position{line: 46, col: 14, offset: 1271},
					expr: &charClassMatcher{
						pos:        position{line: 46, col: 14, offset: 1271},
						val:        "[^ \\t\\n\\r]",
						chars:      []rune{' ', '\t', '\n', '\r'},
						ignoreCase: false,
						inverted:   true,
					},
				},
			},
		},
		{
			name: "MultiLiteralString",
			pos:  position{line: 50, col: 1, offset: 1316},
			expr: &actionExpr{
				pos: position{line: 50, col: 22, offset: 1337},
				run: (*parser).callonMultiLiteralString1,
				expr: &seqExpr{
					pos: position{line: 50, col: 22, offset: 1337},
					exprs: []interface{}{
						&labeledExpr{
							pos:   position{line: 50, col: 22, offset: 1337},
							label: "first",
							expr: &ruleRefExpr{
								pos:  position{line: 50, col: 28, offset: 1343},
								name: "LiteralString",
							},
						},
						&labeledExpr{
							pos:   position{line: 50, col: 42, offset: 1357},
							label: "rest",
							expr: &zeroOrMoreExpr{
								pos: position{line: 50, col: 47, offset: 1362},
								expr: &ruleRefExpr{
									pos:  position{line: 50, col: 48, offset: 1363},
									name: "RestLiteralString",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "LiteralString",
			pos:  position{line: 54, col: 1, offset: 1436},
			expr: &actionExpr{
				pos: position{line: 54, col: 17, offset: 1452},
				run: (*parser).callonLiteralString1,
				expr: &oneOrMoreExpr{
					pos: position{line: 54, col: 17, offset: 1452},
					expr: &charClassMatcher{
						pos:        position{line: 54, col: 17, offset: 1452},
						val:        "[^ \\t\\n\\r,=]",
						chars:      []rune{' ', '\t', '\n', '\r', ',', '='},
						ignoreCase: false,
						inverted:   true,
					},
				},
			},
		},
		{
			name: "RestLiteralString",
			pos:  position{line: 58, col: 1, offset: 1499},
			expr: &choiceExpr{
				pos: position{line: 58, col: 21, offset: 1519},
				alternatives: []interface{}{
					&actionExpr{
						pos: position{line: 58, col: 21, offset: 1519},
						run: (*parser).callonRestLiteralString2,
						expr: &seqExpr{
							pos: position{line: 58, col: 21, offset: 1519},
							exprs: []interface{}{
								&litMatcher{
									pos:        position{line: 58, col: 21, offset: 1519},
									val:        ",",
									ignoreCase: false,
								},
								&labeledExpr{
									pos:   position{line: 58, col: 25, offset: 1523},
									label: "sValue",
									expr: &ruleRefExpr{
										pos:  position{line: 58, col: 32, offset: 1530},
										name: "LiteralString",
									},
								},
							},
						},
					},
					&actionExpr{
						pos: position{line: 60, col: 5, offset: 1570},
						run: (*parser).callonRestLiteralString7,
						expr: &seqExpr{
							pos: position{line: 60, col: 5, offset: 1570},
							exprs: []interface{}{
								&litMatcher{
									pos:        position{line: 60, col: 5, offset: 1570},
									val:        ",",
									ignoreCase: false,
								},
								&labeledExpr{
									pos:   position{line: 60, col: 9, offset: 1574},
									label: "errorLiteralValue",
									expr: &ruleRefExpr{
										pos:  position{line: 60, col: 27, offset: 1592},
										name: "ParamValue",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "ISO_8601",
			pos:  position{line: 64, col: 1, offset: 1683},
			expr: &actionExpr{
				pos: position{line: 64, col: 12, offset: 1694},
				run: (*parser).callonISO_86011,
				expr: &seqExpr{
					pos: position{line: 64, col: 12, offset: 1694},
					exprs: []interface{}{
						&oneOrMoreExpr{
							pos: position{line: 64, col: 12, offset: 1694},
							expr: &charClassMatcher{
								pos:        position{line: 64, col: 12, offset: 1694},
								val:        "[0-9]",
								ranges:     []rune{'0', '9'},
								ignoreCase: false,
								inverted:   false,
							},
						},
						&litMatcher{
							pos:        position{line: 64, col: 19, offset: 1701},
							val:        "-",
							ignoreCase: false,
						},
						&oneOrMoreExpr{
							pos: position{line: 64, col: 23, offset: 1705},
							expr: &charClassMatcher{
								pos:        position{line: 64, col: 23, offset: 1705},
								val:        "[0-9]",
								ranges:     []rune{'0', '9'},
								ignoreCase: false,
								inverted:   false,
							},
						},
						&litMatcher{
							pos:        position{line: 64, col: 30, offset: 1712},
							val:        "-",
							ignoreCase: false,
						},
						&oneOrMoreExpr{
							pos: position{line: 64, col: 34, offset: 1716},
							expr: &charClassMatcher{
								pos:        position{line: 64, col: 34, offset: 1716},
								val:        "[0-9]",
								ranges:     []rune{'0', '9'},
								ignoreCase: false,
								inverted:   false,
							},
						},
						&zeroOrOneExpr{
							pos: position{line: 64, col: 41, offset: 1723},
							expr: &seqExpr{
								pos: position{line: 64, col: 42, offset: 1724},
								exprs: []interface{}{
									&litMatcher{
										pos:        position{line: 64, col: 42, offset: 1724},
										val:        "T",
										ignoreCase: false,
									},
									&oneOrMoreExpr{
										pos: position{line: 64, col: 46, offset: 1728},
										expr: &charClassMatcher{
											pos:        position{line: 64, col: 46, offset: 1728},
											val:        "[0-9]",
											ranges:     []rune{'0', '9'},
											ignoreCase: false,
											inverted:   false,
										},
									},
									&zeroOrOneExpr{
										pos: position{line: 64, col: 53, offset: 1735},
										expr: &seqExpr{
											pos: position{line: 64, col: 54, offset: 1736},
											exprs: []interface{}{
												&litMatcher{
													pos:        position{line: 64, col: 54, offset: 1736},
													val:        ":",
													ignoreCase: false,
												},
												&oneOrMoreExpr{
													pos: position{line: 64, col: 58, offset: 1740},
													expr: &charClassMatcher{
														pos:        position{line: 64, col: 58, offset: 1740},
														val:        "[0-9]",
														ranges:     []rune{'0', '9'},
														ignoreCase: false,
														inverted:   false,
													},
												},
												&zeroOrOneExpr{
													pos: position{line: 64, col: 65, offset: 1747},
													expr: &seqExpr{
														pos: position{line: 64, col: 66, offset: 1748},
														exprs: []interface{}{
															&zeroOrOneExpr{
																pos: position{line: 64, col: 66, offset: 1748},
																expr: &charClassMatcher{
																	pos:        position{line: 64, col: 66, offset: 1748},
																	val:        "[Z+-]",
																	chars:      []rune{'Z', '+', '-'},
																	ignoreCase: false,
																	inverted:   false,
																},
															},
															&oneOrMoreExpr{
																pos: position{line: 64, col: 73, offset: 1755},
																expr: &charClassMatcher{
																	pos:        position{line: 64, col: 73, offset: 1755},
																	val:        "[0-9]",
																	ranges:     []rune{'0', '9'},
																	ignoreCase: false,
																	inverted:   false,
																},
															},
															&zeroOrOneExpr{
																pos: position{line: 64, col: 80, offset: 1762},
																expr: &seqExpr{
																	pos: position{line: 64, col: 81, offset: 1763},
																	exprs: []interface{}{
																		&litMatcher{
																			pos:        position{line: 64, col: 81, offset: 1763},
																			val:        ":",
																			ignoreCase: false,
																		},
																		&oneOrMoreExpr{
																			pos: position{line: 64, col: 85, offset: 1767},
																			expr: &charClassMatcher{
																				pos:        position{line: 64, col: 85, offset: 1767},
																				val:        "[0-9]",
																				ranges:     []rune{'0', '9'},
																				ignoreCase: false,
																				inverted:   false,
																			},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "UNIX_TIME",
			pos:  position{line: 68, col: 1, offset: 1811},
			expr: &actionExpr{
				pos: position{line: 68, col: 13, offset: 1823},
				run: (*parser).callonUNIX_TIME1,
				expr: &oneOrMoreExpr{
					pos: position{line: 68, col: 13, offset: 1823},
					expr: &charClassMatcher{
						pos:        position{line: 68, col: 13, offset: 1823},
						val:        "[0-9]",
						ranges:     []rune{'0', '9'},
						ignoreCase: false,
						inverted:   false,
					},
				},
			},
		},
		{
			name: "_",
			pos:  position{line: 72, col: 1, offset: 1860},
			expr: &zeroOrMoreExpr{
				pos: position{line: 72, col: 5, offset: 1864},
				expr: &ruleRefExpr{
					pos:  position{line: 72, col: 5, offset: 1864},
					name: "EMPTY_CHAR",
				},
			},
		},
		{
			name: "END_WORD",
			pos:  position{line: 73, col: 1, offset: 1876},
			expr: &choiceExpr{
				pos: position{line: 73, col: 12, offset: 1887},
				alternatives: []interface{}{
					&ruleRefExpr{
						pos:  position{line: 73, col: 12, offset: 1887},
						name: "EOF",
					},
					&oneOrMoreExpr{
						pos: position{line: 73, col: 18, offset: 1893},
						expr: &ruleRefExpr{
							pos:  position{line: 73, col: 18, offset: 1893},
							name: "EMPTY_CHAR",
						},
					},
				},
			},
		},
		{
			name: "EMPTY_CHAR",
			pos:  position{line: 74, col: 1, offset: 1905},
			expr: &charClassMatcher{
				pos:        position{line: 74, col: 14, offset: 1918},
				val:        "[ \\t\\n\\r]",
				chars:      []rune{' ', '\t', '\n', '\r'},
				ignoreCase: false,
				inverted:   false,
			},
		},
		{
			name: "EOF",
			pos:  position{line: 75, col: 1, offset: 1928},
			expr: &notExpr{
				pos: position{line: 75, col: 7, offset: 1934},
				expr: &anyMatcher{
					line: 75, col: 8, offset: 1935,
				},
			},
		},
	},
}

func (c *current) onQuery1(allParams interface{}) (interface{}, error) {
	var queryParams QueryParams
	queryParams.ProvinceRelation = UNKNOWN_RELATION

	setParamsError := setParams(&queryParams, allParams)
	if setParamsError != nil {
		return &queryParams, setParamsError
	}

	return &queryParams, setParamsError
}

func (p *parser) callonQuery1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onQuery1(stack["allParams"])
}

func (c *current) onQueryParam2(param interface{}) (interface{}, error) {
	return param, nil
}

func (p *parser) callonQueryParam2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onQueryParam2(stack["param"])
}

func (c *current) onQueryParam10(paramName interface{}) (interface{}, error) {
	return emptyParamContent, fmt.Errorf("Unknown parameter: %q", paramName)
}

func (p *parser) callonQueryParam10() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onQueryParam10(stack["paramName"])
}

func (c *current) onNodeFilter2(nodeProperty, stringValues interface{}) (interface{}, error) {
	return buildParamContent(nodeProperty, stringValues), nil
}

func (p *parser) callonNodeFilter2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onNodeFilter2(stack["nodeProperty"], stack["stringValues"])
}

func (c *current) onNodeFilter9(paramName, assignedValue interface{}) (interface{}, error) {
	return parseValidPramName(paramName, assignedValue)
}

func (p *parser) callonNodeFilter9() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onNodeFilter9(stack["paramName"], stack["assignedValue"])
}

func (c *current) onTimeFilter2(timeParamName, timeValue interface{}) (interface{}, error) {
	return buildParamContent(timeParamName, timeValue), nil
}

func (p *parser) callonTimeFilter2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onTimeFilter2(stack["timeParamName"], stack["timeValue"])
}

func (c *current) onTimeFilter11(paramName, assignedValue interface{}) (interface{}, error) {
	return parseValidPramName(paramName, assignedValue)
}

func (p *parser) callonTimeFilter11() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onTimeFilter11(stack["paramName"], stack["assignedValue"])
}

func (c *current) onNodeProperty1() (interface{}, error) {
	return string(c.text), nil
}

func (p *parser) callonNodeProperty1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onNodeProperty1()
}

func (c *current) onTimeParamName1() (interface{}, error) {
	return string(c.text), nil
}

func (p *parser) callonTimeParamName1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onTimeParamName1()
}

func (c *current) onParamName1() (interface{}, error) {
	return string(c.text), nil
}

func (p *parser) callonParamName1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onParamName1()
}

func (c *current) onParamValue1() (interface{}, error) {
	return string(c.text), nil
}

func (p *parser) callonParamValue1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onParamValue1()
}

func (c *current) onMultiLiteralString1(first, rest interface{}) (interface{}, error) {
	return combineStringLiterals(first, rest), nil
}

func (p *parser) callonMultiLiteralString1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onMultiLiteralString1(stack["first"], stack["rest"])
}

func (c *current) onLiteralString1() (interface{}, error) {
	return string(c.text), nil
}

func (p *parser) callonLiteralString1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onLiteralString1()
}

func (c *current) onRestLiteralString2(sValue interface{}) (interface{}, error) {
	return sValue, nil
}

func (p *parser) callonRestLiteralString2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRestLiteralString2(stack["sValue"])
}

func (c *current) onRestLiteralString7(errorLiteralValue interface{}) (interface{}, error) {
	return "", fmt.Errorf("Illegal literal value: \"%v\"", errorLiteralValue)
}

func (p *parser) callonRestLiteralString7() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRestLiteralString7(stack["errorLiteralValue"])
}

func (c *current) onISO_86011() (interface{}, error) {
	return parseIso8601(c)
}

func (p *parser) callonISO_86011() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onISO_86011()
}

func (c *current) onUNIX_TIME1() (interface{}, error) {
	return parseUnixTime(c)
}

func (p *parser) callonUNIX_TIME1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onUNIX_TIME1()
}

var (
	// errNoRule is returned when the grammar to parse has no rule.
	errNoRule = errors.New("grammar has no rule")

	// errInvalidEncoding is returned when the source is not properly
	// utf8-encoded.
	errInvalidEncoding = errors.New("invalid encoding")

	// errNoMatch is returned if no match could be found.
	errNoMatch = errors.New("no match found")
)

// Option is a function that can set an option on the parser. It returns
// the previous setting as an Option.
type Option func(*parser) Option

// Debug creates an Option to set the debug flag to b. When set to true,
// debugging information is printed to stdout while parsing.
//
// The default is false.
func Debug(b bool) Option {
	return func(p *parser) Option {
		old := p.debug
		p.debug = b
		return Debug(old)
	}
}

// Memoize creates an Option to set the memoize flag to b. When set to true,
// the parser will cache all results so each expression is evaluated only
// once. This guarantees linear parsing time even for pathological cases,
// at the expense of more memory and slower times for typical cases.
//
// The default is false.
func Memoize(b bool) Option {
	return func(p *parser) Option {
		old := p.memoize
		p.memoize = b
		return Memoize(old)
	}
}

// Recover creates an Option to set the recover flag to b. When set to
// true, this causes the parser to recover from panics and convert it
// to an error. Setting it to false can be useful while debugging to
// access the full stack trace.
//
// The default is true.
func Recover(b bool) Option {
	return func(p *parser) Option {
		old := p.recover
		p.recover = b
		return Recover(old)
	}
}

// ParseFile parses the file identified by filename.
func ParseFile(filename string, opts ...Option) (interface{}, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseReader(filename, f, opts...)
}

// ParseReader parses the data from r using filename as information in the
// error messages.
func ParseReader(filename string, r io.Reader, opts ...Option) (interface{}, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return Parse(filename, b, opts...)
}

// Parse parses the data from b using filename as information in the
// error messages.
func Parse(filename string, b []byte, opts ...Option) (interface{}, error) {
	return newParser(filename, b, opts...).parse(g)
}

// position records a position in the text.
type position struct {
	line, col, offset int
}

func (p position) String() string {
	return fmt.Sprintf("%d:%d [%d]", p.line, p.col, p.offset)
}

// savepoint stores all state required to go back to this point in the
// parser.
type savepoint struct {
	position
	rn rune
	w  int
}

type current struct {
	pos  position // start position of the match
	text []byte   // raw text of the match
}

// the AST types...

type grammar struct {
	pos   position
	rules []*rule
}

type rule struct {
	pos         position
	name        string
	displayName string
	expr        interface{}
}

type choiceExpr struct {
	pos          position
	alternatives []interface{}
}

type actionExpr struct {
	pos  position
	expr interface{}
	run  func(*parser) (interface{}, error)
}

type seqExpr struct {
	pos   position
	exprs []interface{}
}

type labeledExpr struct {
	pos   position
	label string
	expr  interface{}
}

type expr struct {
	pos  position
	expr interface{}
}

type andExpr expr
type notExpr expr
type zeroOrOneExpr expr
type zeroOrMoreExpr expr
type oneOrMoreExpr expr

type ruleRefExpr struct {
	pos  position
	name string
}

type andCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type notCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type litMatcher struct {
	pos        position
	val        string
	ignoreCase bool
}

type charClassMatcher struct {
	pos        position
	val        string
	chars      []rune
	ranges     []rune
	classes    []*unicode.RangeTable
	ignoreCase bool
	inverted   bool
}

type anyMatcher position

// errList cumulates the errors found by the parser.
type errList []error

func (e *errList) add(err error) {
	*e = append(*e, err)
}

func (e errList) err() error {
	if len(e) == 0 {
		return nil
	}
	e.dedupe()
	return e
}

func (e *errList) dedupe() {
	var cleaned []error
	set := make(map[string]bool)
	for _, err := range *e {
		if msg := err.Error(); !set[msg] {
			set[msg] = true
			cleaned = append(cleaned, err)
		}
	}
	*e = cleaned
}

func (e errList) Error() string {
	switch len(e) {
	case 0:
		return ""
	case 1:
		return e[0].Error()
	default:
		var buf bytes.Buffer

		for i, err := range e {
			if i > 0 {
				buf.WriteRune('\n')
			}
			buf.WriteString(err.Error())
		}
		return buf.String()
	}
}

// parserError wraps an error with a prefix indicating the rule in which
// the error occurred. The original error is stored in the Inner field.
type parserError struct {
	Inner  error
	pos    position
	prefix string
}

// Error returns the error message.
func (p *parserError) Error() string {
	return p.prefix + ": " + p.Inner.Error()
}

// newParser creates a parser with the specified input source and options.
func newParser(filename string, b []byte, opts ...Option) *parser {
	p := &parser{
		filename: filename,
		errs:     new(errList),
		data:     b,
		pt:       savepoint{position: position{line: 1}},
		recover:  true,
	}
	p.setOptions(opts)
	return p
}

// setOptions applies the options to the parser.
func (p *parser) setOptions(opts []Option) {
	for _, opt := range opts {
		opt(p)
	}
}

type resultTuple struct {
	v   interface{}
	b   bool
	end savepoint
}

type parser struct {
	filename string
	pt       savepoint
	cur      current

	data []byte
	errs *errList

	recover bool
	debug   bool
	depth   int

	memoize bool
	// memoization table for the packrat algorithm:
	// map[offset in source] map[expression or rule] {value, match}
	memo map[int]map[interface{}]resultTuple

	// rules table, maps the rule identifier to the rule node
	rules map[string]*rule
	// variables stack, map of label to value
	vstack []map[string]interface{}
	// rule stack, allows identification of the current rule in errors
	rstack []*rule

	// stats
	exprCnt int
}

// push a variable set on the vstack.
func (p *parser) pushV() {
	if cap(p.vstack) == len(p.vstack) {
		// create new empty slot in the stack
		p.vstack = append(p.vstack, nil)
	} else {
		// slice to 1 more
		p.vstack = p.vstack[:len(p.vstack)+1]
	}

	// get the last args set
	m := p.vstack[len(p.vstack)-1]
	if m != nil && len(m) == 0 {
		// empty map, all good
		return
	}

	m = make(map[string]interface{})
	p.vstack[len(p.vstack)-1] = m
}

// pop a variable set from the vstack.
func (p *parser) popV() {
	// if the map is not empty, clear it
	m := p.vstack[len(p.vstack)-1]
	if len(m) > 0 {
		// GC that map
		p.vstack[len(p.vstack)-1] = nil
	}
	p.vstack = p.vstack[:len(p.vstack)-1]
}

func (p *parser) print(prefix, s string) string {
	if !p.debug {
		return s
	}

	fmt.Printf("%s %d:%d:%d: %s [%#U]\n",
		prefix, p.pt.line, p.pt.col, p.pt.offset, s, p.pt.rn)
	return s
}

func (p *parser) in(s string) string {
	p.depth++
	return p.print(strings.Repeat(" ", p.depth)+">", s)
}

func (p *parser) out(s string) string {
	p.depth--
	return p.print(strings.Repeat(" ", p.depth)+"<", s)
}

func (p *parser) addErr(err error) {
	p.addErrAt(err, p.pt.position)
}

func (p *parser) addErrAt(err error, pos position) {
	var buf bytes.Buffer
	if p.filename != "" {
		buf.WriteString(p.filename)
	}
	if buf.Len() > 0 {
		buf.WriteString(":")
	}
	buf.WriteString(fmt.Sprintf("%d:%d (%d)", pos.line, pos.col, pos.offset))
	if len(p.rstack) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(": ")
		}
		rule := p.rstack[len(p.rstack)-1]
		if rule.displayName != "" {
			buf.WriteString("rule " + rule.displayName)
		} else {
			buf.WriteString("rule " + rule.name)
		}
	}
	pe := &parserError{Inner: err, prefix: buf.String()}
	p.errs.add(pe)
}

// read advances the parser to the next rune.
func (p *parser) read() {
	p.pt.offset += p.pt.w
	rn, n := utf8.DecodeRune(p.data[p.pt.offset:])
	p.pt.rn = rn
	p.pt.w = n
	p.pt.col++
	if rn == '\n' {
		p.pt.line++
		p.pt.col = 0
	}

	if rn == utf8.RuneError {
		if n > 0 {
			p.addErr(errInvalidEncoding)
		}
	}
}

// restore parser position to the savepoint pt.
func (p *parser) restore(pt savepoint) {
	if p.debug {
		defer p.out(p.in("restore"))
	}
	if pt.offset == p.pt.offset {
		return
	}
	p.pt = pt
}

// get the slice of bytes from the savepoint start to the current position.
func (p *parser) sliceFrom(start savepoint) []byte {
	return p.data[start.position.offset:p.pt.position.offset]
}

func (p *parser) getMemoized(node interface{}) (resultTuple, bool) {
	if len(p.memo) == 0 {
		return resultTuple{}, false
	}
	m := p.memo[p.pt.offset]
	if len(m) == 0 {
		return resultTuple{}, false
	}
	res, ok := m[node]
	return res, ok
}

func (p *parser) setMemoized(pt savepoint, node interface{}, tuple resultTuple) {
	if p.memo == nil {
		p.memo = make(map[int]map[interface{}]resultTuple)
	}
	m := p.memo[pt.offset]
	if m == nil {
		m = make(map[interface{}]resultTuple)
		p.memo[pt.offset] = m
	}
	m[node] = tuple
}

func (p *parser) buildRulesTable(g *grammar) {
	p.rules = make(map[string]*rule, len(g.rules))
	for _, r := range g.rules {
		p.rules[r.name] = r
	}
}

func (p *parser) parse(g *grammar) (val interface{}, err error) {
	if len(g.rules) == 0 {
		p.addErr(errNoRule)
		return nil, p.errs.err()
	}

	// TODO : not super critical but this could be generated
	p.buildRulesTable(g)

	if p.recover {
		// panic can be used in action code to stop parsing immediately
		// and return the panic as an error.
		defer func() {
			if e := recover(); e != nil {
				if p.debug {
					defer p.out(p.in("panic handler"))
				}
				val = nil
				switch e := e.(type) {
				case error:
					p.addErr(e)
				default:
					p.addErr(fmt.Errorf("%v", e))
				}
				err = p.errs.err()
			}
		}()
	}

	// start rule is rule [0]
	p.read() // advance to first rune
	val, ok := p.parseRule(g.rules[0])
	if !ok {
		if len(*p.errs) == 0 {
			// make sure this doesn't go out silently
			p.addErr(errNoMatch)
		}
		return nil, p.errs.err()
	}
	return val, p.errs.err()
}

func (p *parser) parseRule(rule *rule) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseRule " + rule.name))
	}

	if p.memoize {
		res, ok := p.getMemoized(rule)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
	}

	start := p.pt
	p.rstack = append(p.rstack, rule)
	p.pushV()
	val, ok := p.parseExpr(rule.expr)
	p.popV()
	p.rstack = p.rstack[:len(p.rstack)-1]
	if ok && p.debug {
		p.print(strings.Repeat(" ", p.depth)+"MATCH", string(p.sliceFrom(start)))
	}

	if p.memoize {
		p.setMemoized(start, rule, resultTuple{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseExpr(expr interface{}) (interface{}, bool) {
	var pt savepoint
	var ok bool

	if p.memoize {
		res, ok := p.getMemoized(expr)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
		pt = p.pt
	}

	p.exprCnt++
	var val interface{}
	switch expr := expr.(type) {
	case *actionExpr:
		val, ok = p.parseActionExpr(expr)
	case *andCodeExpr:
		val, ok = p.parseAndCodeExpr(expr)
	case *andExpr:
		val, ok = p.parseAndExpr(expr)
	case *anyMatcher:
		val, ok = p.parseAnyMatcher(expr)
	case *charClassMatcher:
		val, ok = p.parseCharClassMatcher(expr)
	case *choiceExpr:
		val, ok = p.parseChoiceExpr(expr)
	case *labeledExpr:
		val, ok = p.parseLabeledExpr(expr)
	case *litMatcher:
		val, ok = p.parseLitMatcher(expr)
	case *notCodeExpr:
		val, ok = p.parseNotCodeExpr(expr)
	case *notExpr:
		val, ok = p.parseNotExpr(expr)
	case *oneOrMoreExpr:
		val, ok = p.parseOneOrMoreExpr(expr)
	case *ruleRefExpr:
		val, ok = p.parseRuleRefExpr(expr)
	case *seqExpr:
		val, ok = p.parseSeqExpr(expr)
	case *zeroOrMoreExpr:
		val, ok = p.parseZeroOrMoreExpr(expr)
	case *zeroOrOneExpr:
		val, ok = p.parseZeroOrOneExpr(expr)
	default:
		panic(fmt.Sprintf("unknown expression type %T", expr))
	}
	if p.memoize {
		p.setMemoized(pt, expr, resultTuple{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseActionExpr(act *actionExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseActionExpr"))
	}

	start := p.pt
	val, ok := p.parseExpr(act.expr)
	if ok {
		p.cur.pos = start.position
		p.cur.text = p.sliceFrom(start)
		actVal, err := act.run(p)
		if err != nil {
			p.addErrAt(err, start.position)
		}
		val = actVal
	}
	if ok && p.debug {
		p.print(strings.Repeat(" ", p.depth)+"MATCH", string(p.sliceFrom(start)))
	}
	return val, ok
}

func (p *parser) parseAndCodeExpr(and *andCodeExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAndCodeExpr"))
	}

	ok, err := and.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, ok
}

func (p *parser) parseAndExpr(and *andExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAndExpr"))
	}

	pt := p.pt
	p.pushV()
	_, ok := p.parseExpr(and.expr)
	p.popV()
	p.restore(pt)
	return nil, ok
}

func (p *parser) parseAnyMatcher(any *anyMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAnyMatcher"))
	}

	if p.pt.rn != utf8.RuneError {
		start := p.pt
		p.read()
		return p.sliceFrom(start), true
	}
	return nil, false
}

func (p *parser) parseCharClassMatcher(chr *charClassMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseCharClassMatcher"))
	}

	cur := p.pt.rn
	// can't match EOF
	if cur == utf8.RuneError {
		return nil, false
	}
	start := p.pt
	if chr.ignoreCase {
		cur = unicode.ToLower(cur)
	}

	// try to match in the list of available chars
	for _, rn := range chr.chars {
		if rn == cur {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of ranges
	for i := 0; i < len(chr.ranges); i += 2 {
		if cur >= chr.ranges[i] && cur <= chr.ranges[i+1] {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of Unicode classes
	for _, cl := range chr.classes {
		if unicode.Is(cl, cur) {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	if chr.inverted {
		p.read()
		return p.sliceFrom(start), true
	}
	return nil, false
}

func (p *parser) parseChoiceExpr(ch *choiceExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseChoiceExpr"))
	}

	for _, alt := range ch.alternatives {
		p.pushV()
		val, ok := p.parseExpr(alt)
		p.popV()
		if ok {
			return val, ok
		}
	}
	return nil, false
}

func (p *parser) parseLabeledExpr(lab *labeledExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseLabeledExpr"))
	}

	p.pushV()
	val, ok := p.parseExpr(lab.expr)
	p.popV()
	if ok && lab.label != "" {
		m := p.vstack[len(p.vstack)-1]
		m[lab.label] = val
	}
	return val, ok
}

func (p *parser) parseLitMatcher(lit *litMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseLitMatcher"))
	}

	start := p.pt
	for _, want := range lit.val {
		cur := p.pt.rn
		if lit.ignoreCase {
			cur = unicode.ToLower(cur)
		}
		if cur != want {
			p.restore(start)
			return nil, false
		}
		p.read()
	}
	return p.sliceFrom(start), true
}

func (p *parser) parseNotCodeExpr(not *notCodeExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseNotCodeExpr"))
	}

	ok, err := not.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, !ok
}

func (p *parser) parseNotExpr(not *notExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseNotExpr"))
	}

	pt := p.pt
	p.pushV()
	_, ok := p.parseExpr(not.expr)
	p.popV()
	p.restore(pt)
	return nil, !ok
}

func (p *parser) parseOneOrMoreExpr(expr *oneOrMoreExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseOneOrMoreExpr"))
	}

	var vals []interface{}

	for {
		p.pushV()
		val, ok := p.parseExpr(expr.expr)
		p.popV()
		if !ok {
			if len(vals) == 0 {
				// did not match once, no match
				return nil, false
			}
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseRuleRefExpr(ref *ruleRefExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseRuleRefExpr " + ref.name))
	}

	if ref.name == "" {
		panic(fmt.Sprintf("%s: invalid rule: missing name", ref.pos))
	}

	rule := p.rules[ref.name]
	if rule == nil {
		p.addErr(fmt.Errorf("undefined rule: %s", ref.name))
		return nil, false
	}
	return p.parseRule(rule)
}

func (p *parser) parseSeqExpr(seq *seqExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseSeqExpr"))
	}

	var vals []interface{}

	pt := p.pt
	for _, expr := range seq.exprs {
		val, ok := p.parseExpr(expr)
		if !ok {
			p.restore(pt)
			return nil, false
		}
		vals = append(vals, val)
	}
	return vals, true
}

func (p *parser) parseZeroOrMoreExpr(expr *zeroOrMoreExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrMoreExpr"))
	}

	var vals []interface{}

	for {
		p.pushV()
		val, ok := p.parseExpr(expr.expr)
		p.popV()
		if !ok {
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseZeroOrOneExpr(expr *zeroOrOneExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrOneExpr"))
	}

	p.pushV()
	val, _ := p.parseExpr(expr.expr)
	p.popV()
	// whether it matched or not, consider it a match
	return val, true
}

func rangeTable(class string) *unicode.RangeTable {
	if rt, ok := unicode.Categories[class]; ok {
		return rt
	}
	if rt, ok := unicode.Properties[class]; ok {
		return rt
	}
	if rt, ok := unicode.Scripts[class]; ok {
		return rt
	}

	// cannot happen
	panic(fmt.Sprintf("invalid Unicode class: %s", class))
}
