# Approach
1. pass map, not KW args; saves massively on marshall/unmarshall boilerplate
2. thinning functions down like this will help to show where duplicated code exists
3. keep the bulk of everything functional, transact/mutate/write at the top
4. Decomplect ALL THE THINGS

# General notes
* transact and emit edn only at top; everything below should be functional
* schema gathering entirely separate from data gathering? I think so.
* I think I half follow what's happening with the history / timecreated bits
* Make defaults conform to config conform to arguments. Flow through as map from the top. 'merge them together and the right values will result.
* 
