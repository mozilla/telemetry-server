printjson(db.payloads.mapReduce(
  function() { emit(this.info.OS, 1);},
  function(key, values) { return Array.sum(values);},
  {
    out: { inline: 1}
  }))
