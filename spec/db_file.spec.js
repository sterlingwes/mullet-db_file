var DBFS = require('../main.js')({
    path:   __dirname + '/testfs'
})

  , DB = require('../../db/main.js')(DBFS)

  , Schema = DB.schema('test', {
      fields: {
          text: {
              type: String,
              safe: true
          }
      }
  })

  , schema = new Schema({
      text: 'Hello World'
  });

describe('DB (FS Interface)', function() {
    
    it('should insert records', function(done) {
        
        DBFS.insert('test')(schema, function(err,res) {
            expect(err).toBeFalsy();
            expect(res).toEqual(schema);
            done();
        });
        
    });
    
    it('should find records', function(done) {
        
        DBFS.find('test')({ text: 'Hello World' }, function(err,res) {
            expect(res[0]._id).toEqual(schema.id);
            expect(DBFS._index).toEqual({test:[schema.id]});
            done();
        });
        
    });
    
    it('should remove records', function(done) {
        
        DBFS.remove('test')({}, function(err) {
            expect(DBFS._index, {});
            done();
        });
        
    });
    
});