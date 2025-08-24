# atomdb

AtomDB is a reliable, simple, fast, no-frills key-value database.

## 🎯 Design Goals

- ✅ **No data loss** - Immediate disk persistence with atomic writes
- ✅ **No WAL complexity** - Eliminates buffer races and checkpoint issues
- ✅ **Simple architecture** - Memory + disk, that's it
- ✅ **High reliability** - Fewer moving parts = fewer bugs
- ✅ **Easy testing** - Comprehensive test suite included

## 🔧 Usage Example

```typescript
import atomdb from './atomdb';

const db = new atomdb({
  databaseDirectory: './my-database'
});

await db.start();

// Write data (immediately persisted to disk)
await db.write('users', 'john', { name: 'John', age: 30 });

// Read data
const user = await db.get('users', 'john');
console.log(user); // { name: 'John', age: 30 }

// Get all data from table
const allUsers = await db.get('users');
console.log(allUsers); // [['john', { name: 'John', age: 30 }]]

// Delete data
await db.delete('users', 'john');

// Clean shutdown
await db.close();
```
