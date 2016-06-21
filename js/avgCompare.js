/*
  Get result that bigger or equal or small than the avg value of counter from all records
*/
cond = (typeof cond == "undefined"? ">" : cond)
if(cond.match(/[=><]+/g).size === 0){
  cond = ">"
}
avg = 0
t2 = _.map(input, function(res){
  res.Avg = _.reduce(res.Values, function(sum,v){
    return (sum+v.Value)
  },0) / (res.Values.length === 0 ? 1 : res.Values.length)
  avg += res.Avg
  return res;
})

avg = avg/t2.length
console.log("current avg number: " + avg)
t3 = _.filter(t2, function(x){
  var condbool
  if(eval("x.Avg " + cond + " avg")){
    return x.Endpoint, x.Avg
  }
})
output = JSON.stringify(t3)
