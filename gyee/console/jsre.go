/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  The gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  The gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package console

import (
	"github.com/robertkrimen/otto"
	"strings"
	"sort"
)

// JSRE javascript runtime environment
type JSRE struct {

	// the representation of the JavaScript runtime
	vm *otto.Otto
}

func newJSRE() *JSRE {
	jsre := &JSRE{
		vm: otto.New(),
	}
	return jsre
}

// Run will run the given source (parsing it first if necessary), returning the resulting value and error (if any)
func (j *JSRE) Run(src string) (otto.Value, error) {
	return j.vm.Run(src)
}

// Get returns the value of a variable in the JS environment.
func (j *JSRE) Get(name string) (otto.Value, error) {
	return j.vm.Get(name)
}

// Set assigns value v to a variable in the JS environment.
func (j *JSRE) Set(name string, value interface{}) error {
	return j.vm.Set(name, value)
}

// Compile compiles and then runs JS code.
func (j *JSRE) Compile(filename string, src interface{}) error {
	script, err := j.vm.Compile(filename, src)
	if err != nil {
		return err
	}
	_, err = j.vm.Run(script)
	return err
}

// JSONString convert value to json string
func (j *JSRE) JSONString(val otto.Value) (string, error) {
	JSON, _ := j.vm.Object("JSON")
	jsonVal, err := JSON.Call("stringify", val)
	if err != nil {
		return "", err
	}
	return jsonVal.String(), nil
}

// CompleteKeywords returns potential continuations for the given line.
func (j *JSRE) CompleteKeywords(line string) []string {
	parts := strings.Split(line, ".")
	objRef := "this"
	prefix := line
	if len(parts) > 1 {
		objRef = strings.Join(parts[0:len(parts)-1], ".")
		prefix = parts[len(parts)-1]
	}

	obj, _ := j.vm.Object(objRef)
	if obj == nil {
		return nil
	}
	properties := j.getObjectKeys(obj, objRef, prefix)
	// only not golbal prototype should be use
	if objRef != "this" {
		if c, _ := obj.Get("constructor"); c.Object() != nil {
			if p, _ := c.Object().Get("prototype"); p.Object() != nil {
				keys := j.getObjectKeys(p.Object(), objRef, prefix)
				// remove the duplicate property
				set := make(map[string]bool)
				for _, key := range keys {
					set[key] = true
				}
				for _, key := range properties {
					set[key] = true
				}
				properties = make([]string, 0, len(set))
				for k := range set {
					properties = append(properties, k)
				}
			}
		}
	}
	tmp := make([]string, len(properties))
	copy(tmp, properties)
	for _, v := range tmp {
		tmps := strings.Split(v, ".")
		f := tmps[len(tmps)-1]
		// only out property use,remove request func
		if f == "request" || f == "constructor" || strings.HasPrefix(f, "_") {
			properties = sliceRemove(properties, v)
		}
	}
	// Append opening parenthesis (for functions) or dot (for objects)
	// if the line itself is the only completion.
	if len(properties) == 1 && properties[0] == line {
		obj, _ := j.vm.Object(line)
		if obj != nil {
			if obj.Class() == "Function" {
				properties[0] += "()"
			} else {
				properties[0] += "."
			}
		}
	}
	sort.Strings(properties)
	return properties
}

func (j *JSRE) getObjectKeys(obj *otto.Object, objRef, prefix string) (properties []string) {
	Object, _ := j.vm.Object("Object")
	rv, _ := Object.Call("getOwnPropertyNames", obj.Value())
	gv, _ := rv.Export()
	switch gv := gv.(type) {
	case []string:
		properties = parseOwnKeys(objRef, prefix, gv)
	}
	return properties
}

func parseOwnKeys(objRef, prefix string, properties []string) []string {
	//fmt.Println("parse keys:", properties)
	var results []string
	for _, property := range properties {
		//fmt.Println("property is:", property)
		if len(prefix) == 0 || strings.HasPrefix(property, prefix) {
			if objRef == "this" {
				results = append(results, property)
			} else {
				results = append(results, objRef+"."+property)
			}
		}
	}
	return results
}

func sliceRemove(slices []string, value string) []string {
	for i, v := range slices {
		if v == value {
			slices = append(slices[:i], slices[i+1:]...)
			break
		}
	}
	return slices
}

//TODO:看是否需要搞到单独的包中