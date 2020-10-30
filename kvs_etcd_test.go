package hmetcd

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	var kvh Kvi
	var err error

	t.Logf("** TESTING OPEN/CLOSE **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED OPEN/CLOSE TEST\n")
}

func TestDistLock(t *testing.T) {
	var kvh Kvi
	var err error

	t.Logf("** TESTING DIST LOCKS **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	//This test is a little hokey in that we can't test multi-locking,
	//since ETCD will let a process grab the lock multiple times.  So,
	//we'll just insure that lock/unlock works.

	err = kvh.DistLock()
	if err != nil {
		t.Error("ERROR acquiring distributed lock:", err)
	}

	err = kvh.DistUnlock()
	if err != nil {
		t.Error("ERROR releasing distributed lock:", err)
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED DIST LOCK TEST\n")
}

func TestStore(t *testing.T) {
	var kvh Kvi
	var err error
	var kkey, kval string

	t.Logf("** RUNNING STORE OPERATIONS TEST **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	//Store 1000 keys

	for i := 0; i < 1000; i += 1 {
		kkey = fmt.Sprintf("key_%04d", i)
		kval = fmt.Sprintf("val_%04d", i)

		err = kvh.Store(kkey, kval)
		if err != nil {
			t.Error("ERROR storing key:", kkey, ":", err)
			break
		}
	}

	//We'll leave the values in place, but close the interface.

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED STORE OPERATIONS TEST\n")
}

func TestGet(t *testing.T) {
	var kvh Kvi
	var err error
	var i int
	var ok bool
	var kkey, kval, ekval string

	t.Logf("** RUNNING GET OPERATIONS TEST **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	//Write the KVs

	for i = 0; i < 1000; i += 1 {
		kkey = fmt.Sprintf("key_%04d", i)
		kval = fmt.Sprintf("val_%04d", i)

		err = kvh.Store(kkey, kval)
		if err != nil {
			t.Error("ERROR storing key:", kkey, ":", err)
			break
		}
	}

	//Read them back

	for i = 0; i < 1000; i += 1 {
		kkey = fmt.Sprintf("key_%04d", i)
		kval, ok, err = kvh.Get(kkey)
		if err != nil {
			t.Error("ERROR fetching key:", kkey, ":", err)
			break
		}
		if ok != true {
			t.Error("ERROR, Get operation on known key says key doesn't exist.")
		}

		ekval = fmt.Sprintf("val_%04d", i)
		if kval != ekval {
			t.Error("ERROR, mismatch, expected:", ekval, ", got:", kval)
			break
		}
	}

	//Test reading a non-existent key

	kval, ok, err = kvh.Get("xyzzy_xyzzy")
	if err != nil {
		t.Error("ERROR, reading empty key should not fail, got:", err)
	}
	if ok == true {
		t.Error("ERROR, reading empty key should return a false key-exists flag.")
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED GET OPERATIONS TEST\n")
}

func TestGetRange(t *testing.T) {
	var kvh Kvi
	var err error
	var i int
	var kkey, kval string
	var kvrange []Kvi_KV

	t.Logf("** RUNNING GET RANGE TEST **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	//Write some KVs

	for i = 0; i < 1000; i += 1 {
		kkey = fmt.Sprintf("key_%04d", i)
		kval = fmt.Sprintf("val_%04d", i)

		err = kvh.Store(kkey, kval)
		if err != nil {
			t.Error("ERROR storing key:", kkey, ":", err)
			break
		}
	}

	//Get a range of them

	kvrange, err = kvh.GetRange("key_0100", "key_0500")
	if err != nil {
		t.Error("ERROR getting range:", err)
	}

	//Sort keys lexicographically

	sort.SliceStable(kvrange, func(i, j int) bool { return kvrange[i].Key < kvrange[j].Key })

	//Verify

	for i = 100; i <= 500; i += 1 {
		z := i - 100
		kkey = fmt.Sprintf("key_%04d", i)
		kval = fmt.Sprintf("val_%04d", i)

		if kkey != kvrange[z].Key {
			t.Errorf("ERROR, key mismatch, expected '%s', got '%s'\n", kkey, kvrange[z].Key)
			break
		}
		if kval != kvrange[z].Value {
			t.Errorf("ERROR, value mismatch, expected '%s', got '%s'\n", kval, kvrange[z].Value)
			break
		}
	}

	//Test fetching a range with no keys

	kvrange, err = kvh.GetRange("key_AAAA", "key_BBBB")
	if err != nil {
		t.Error("ERROR, GetRange returned error on empty set:", err)
	}
	if len(kvrange) != 0 {
		t.Error("ERROR< GetRange on empty set returned keys!")
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED GET RANGE TEST\n")
}

func TestDelete(t *testing.T) {
	var kvh Kvi
	var err error
	var i int
	var ok bool
	var kkey, kval string

	t.Logf("** RUNNING DELETE OPERATIONS TEST **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	//Write the KVs

	for i = 0; i < 100; i += 1 {
		kkey = fmt.Sprintf("key_%04d", i)
		kval = fmt.Sprintf("val_%04d", i)

		err = kvh.Store(kkey, kval)
		if err != nil {
			t.Error("ERROR storing key:", kkey, ":", err)
			break
		}
	}

	// Delete one

	err = kvh.Delete("key_0023")
	if err != nil {
		t.Error("ERROR deleting key:", err)
	}

	//Try to access the key, 'ok' should be false (key doesn't exist)

	kval, ok, err = kvh.Get("key_0023")
	if err != nil {
		t.Error("ERROR reading a deleted key unexpected failure.")
	}
	if ok == true {
		t.Error("ERROR, reading a deleted key showed that it still exists.")
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED DELETE OPERATIONS TEST\n")
}

type transdata struct {
	ifkey   string
	inival  string
	op      string
	ifval   string
	thenkey string
	thenval string
	elsekey string
	elseval string
	isthen  bool
}

type tasdata struct {
	key       string
	inival    string
	testval   string
	setval    string
	shouldset bool
}

var trans = []transdata{
	transdata{"x0001", "val0001", "=", "val0001", "x0002", "0002_then", "x0003", "0003_else", true},
	transdata{"x0011", "val0011", "!=", "val0011", "x0012", "0012_then", "x0013", "0013_else", false},
	transdata{"x0021", "val0021", "<", "val0021", "x0022", "0022_then", "x0023", "0023_else", false},
	transdata{"x0031", "val0031", ">", "val0031", "x0032", "0032_then", "x0033", "0033_else", false},
	transdata{"x0041", "val0041", "!=", "val004x", "x0042", "0042_then", "x0043", "0043_else", true},
	transdata{"x0051", "val0051", "<", "val0052", "x0052", "0052_then", "x0053", "0053_else", true},
	transdata{"x0061", "val0061", "<", "val0060", "x0062", "0062_then", "x0063", "0063_else", false},
	transdata{"x0071", "val0071", ">", "val0070", "x0072", "0072_then", "x0073", "0073_else", true},
	transdata{"x0081", "val0081", ">", "val0082", "x0082", "0082_then", "x0083", "0083_else", false},
}

var tass = []tasdata{
	tasdata{"x0100", "val0100", "val0100", "val_0100_tasok", true},
	tasdata{"x0110", "val0110", "val01x0", "val_0110_tasok", false},
}

func TestTransaction(t *testing.T) {
	var kvh Kvi
	var err error
	var i int
	var kkey, kval, rval string
	var bval bool

	t.Logf("** RUNNING TRANSACTION OPERATIONS TEST **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	for i = 0; i < len(trans); i += 1 {
		err = kvh.Store(trans[i].ifkey, trans[i].inival)
		if err != nil {
			t.Error("ERROR storing key:", err)
			break
		}
	}

	for i = 0; i < len(trans); i += 1 {
		bval, err = kvh.Transaction(trans[i].ifkey, trans[i].op,
			trans[i].ifval, trans[i].thenkey,
			trans[i].thenval, trans[i].elsekey,
			trans[i].elseval)
		if err != nil {
			t.Error("ERROR on transaction dataset ", i, ":", err)
		}
		if trans[i].isthen != bval {
			t.Errorf("ERROR, transaction result mismatch on dataset %d\n", i)
		}
	}

	//Double-verify using Get operations

	for i = 0; i < len(trans); i += 1 {
		if trans[i].isthen {
			kkey = trans[i].thenkey
			kval = trans[i].thenval
		} else {
			kkey = trans[i].elsekey
			kval = trans[i].elseval
		}
		rval, _, err = kvh.Get(kkey)
		if err != nil {
			t.Error("ERROR fetching key:", kkey, ":", err)
		}

		if rval != kval {
			t.Errorf("ERROR, double-check mismatch, dataset %d\n", i)
		}
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED TRANSACTION OPERATIONS TEST\n")
}

func TestTAS(t *testing.T) {
	var kvh Kvi
	var err error
	var i int
	var bval bool
	var kval string

	t.Logf("** RUNNING TAS OPERATIONS TEST **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	for i = 0; i < len(tass); i += 1 {
		err = kvh.Store(tass[i].key, tass[i].inival)
		if err != nil {
			t.Error("ERROR storing key:", err)
			break
		}
	}

	//Do the TASs

	for i = 0; i < len(tass); i += 1 {
		bval, err = kvh.TAS(tass[i].key, tass[i].testval, tass[i].setval)
		if err != nil {
			t.Error("ERROR, TAS operation on dataset ", i, " failed:", err)
		}
		if tass[i].shouldset != bval {
			t.Errorf("ERROR, mismatch on dataset %d, expected %t, got %t\n",
				i, tass[i].shouldset, bval)
		}
	}

	//Double check via Get operations

	for i = 0; i < len(tass); i += 1 {
		kval, _, err = kvh.Get(tass[i].key)
		if tass[i].shouldset {
			if kval != tass[i].setval {
				t.Errorf("ERROR, mismatch during double-check, dataset %d, key '%s' set to '%s, expected '%s'\n",
					i, tass[i].key, kval, tass[i].setval)
			}
		} else {
			if kval != tass[i].inival {
				t.Errorf("ERROR, mismatch during double-check, dataset %d, key '%s' set to '%s, expected '%s'\n",
					i, tass[i].key, kval, tass[i].inival)
			}
		}
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED TAS OPERATIONS TEST\n")
}

type watchdata struct {
	key       string
	inival    string
	changeval string
	expop     int
	hit       bool
	actualop  int
}

var watches = []watchdata{
	watchdata{"x0200", "val_0200", "change_0200", KVC_KEYCHANGE_PUT, false, 0},
	watchdata{"x0201", "", "change_0200", KVC_KEYCHANGE_PUT, false, 0},
	watchdata{"x0202", "", "", KVC_KEYCHANGE_DELETE, false, 0},
}

func watchme(t *testing.T, kvh Kvi, wd *watchdata) {
	_, op := kvh.Watch(wd.key)
	wd.actualop = op
	wd.hit = true
}

func watch_cb(key string, val string, op int, udata interface{}) bool {
	wd := udata.(*watchdata)
	wd.hit = true
	wd.actualop = op
	return true
}

func watch_cb_self_cancel(key string, val string, op int, udata interface{}) bool {
	wd := udata.(*watchdata)
	wd.hit = true
	wd.actualop = op
	return false
}

func TestWatch(t *testing.T) {
	var kvh Kvi
	var err error

	t.Logf("** RUNNING WATCH TEST **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	//Set up a key

	err = kvh.Store(watches[0].key, watches[0].inival)
	if err != nil {
		t.Error("ERROR setting up a key:", watches[0].key, ":", err)
	}

	//Set up a watch for that key

	go watchme(t, kvh, &watches[0])

	//Sleep a tick, then change the value

	time.Sleep(500000 * time.Microsecond)
	err = kvh.Store(watches[0].key, watches[0].changeval)
	if err != nil {
		t.Error("ERROR changing key after watch was set:", err)
	}
	time.Sleep(1 * time.Second)

	//Verify that the watch caught it

	if !watches[0].hit {
		t.Errorf("ERROR, no watch was tripped for existing key '%s'\n", watches[0].key)
	}
	if watches[0].expop != watches[0].actualop {
		t.Errorf("ERROR, watch trip for key '%s' unexpected operation, wanted %d, got %d\n",
			watches[0].key, watches[0].expop, watches[0].actualop)
	}

	//Set up a watch for a non-existent key

	go watchme(t, kvh, &watches[1])

	//Sleep a tick, then set a value to that key

	time.Sleep(500000 * time.Microsecond)
	err = kvh.Store(watches[1].key, watches[1].changeval)
	if err != nil {
		t.Error("ERROR changing key after watch was set:", err)
	}
	time.Sleep(1 * time.Second)

	//Verify that it got caught.

	if !watches[1].hit {
		t.Errorf("ERROR, no watch was tripped for formerly non-existent key '%s'\n",
			watches[1].key)
	}
	if watches[1].expop != watches[1].actualop {
		t.Errorf("ERROR, watch trip for key '%s' unexpected operation, wanted %d, got %d\n",
			watches[1].key, watches[1].expop, watches[1].actualop)
	}

	//Set up a watch for deletion of a key

	err = kvh.Store(watches[2].key, watches[2].inival)
	if err != nil {
		t.Error("ERROR setting up a key:", watches[0].key, ":", err)
	}
	time.Sleep(500000 * time.Microsecond)

	go watchme(t, kvh, &watches[2])

	//Sleep a tick, then delete the key

	time.Sleep(500000 * time.Microsecond)
	err = kvh.Delete(watches[2].key)
	if err != nil {
		t.Error("ERROR deleting key:", err)
	}
	time.Sleep(1 * time.Second)

	//Verify that it got caught.

	if !watches[2].hit {
		t.Errorf("ERROR, no watch was tripped for deleted key '%s'\n",
			watches[2].key)
	}
	if watches[2].expop != watches[2].actualop {
		t.Errorf("ERROR, watch trip for key '%s' unexpected operation, wanted %d, got %d\n",
			watches[2].key, watches[2].expop, watches[2].actualop)
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED WATCH TEST\n")
}

// Also tests WatchCBCancel

func TestWatchWithCB(t *testing.T) {
	var kvh Kvi
	var err error
	var i int
	var cbh WatchCBHandle

	t.Logf("** RUNNING WATCH WITH CALLBACK TEST **\n")
	kvh, err = Open("mem:", "")
	if err != nil {
		t.Error("ERROR opening memory-based KV interface:", err)
	}

	//clear all hit indicators from test data, all existing watch keys

	for i = 0; i < len(watches); i += 1 {
		watches[i].hit = false
		watches[i].actualop = 0
		_ = kvh.Delete(watches[i].key)
	}

	//Set up a key

	err = kvh.Store(watches[0].key, watches[0].inival)
	if err != nil {
		t.Error("ERROR setting up a key:", watches[0].key, ":", err)
	}

	//Set up a watch for that key, with callback func

	cbh, err = kvh.WatchWithCB(watches[0].key, KVC_KEYCHANGE_PUT, watch_cb,
		&watches[0])
	if err != nil {
		t.Error("ERROR setting up watch with callback[0]:", err)
	}

	//Sleep a tick, then change the value

	time.Sleep(500000 * time.Microsecond)
	err = kvh.Store(watches[0].key, watches[0].changeval)
	if err != nil {
		t.Error("ERROR changing key after watch was set:", err)
	}
	time.Sleep(1 * time.Second)

	//Verify that the watch caught it

	if !watches[0].hit {
		t.Errorf("ERROR, no watch callback was tripped for existing key '%s'\n", watches[0].key)
	}
	if watches[0].expop != watches[0].actualop {
		t.Errorf("ERROR, watch trip for key '%s' unexpected operation, wanted %d, got %d\n",
			watches[0].key, watches[0].expop, watches[0].actualop)
	}

	//Cancel callback

	kvh.WatchCBCancel(cbh)

	//Set up a watch for a non-existent key, self cancelling CB

	cbh, err = kvh.WatchWithCB(watches[1].key, KVC_KEYCHANGE_PUT,
		watch_cb_self_cancel, &watches[1])
	if err != nil {
		t.Error("ERROR setting up watch with callback[1]:", err)
	}

	//Sleep a tick, then set a value to that key

	time.Sleep(500000 * time.Microsecond)
	err = kvh.Store(watches[1].key, watches[1].changeval)
	if err != nil {
		t.Error("ERROR changing key after watch was set:", err)
	}
	time.Sleep(1 * time.Second)

	//Verify that it got caught.

	if !watches[1].hit {
		t.Errorf("ERROR, no watch callback was tripped for formerly non-existent key '%s'\n",
			watches[1].key)
	}
	if watches[1].expop != watches[1].actualop {
		t.Errorf("ERROR, watch trip for key '%s' unexpected operation, wanted %d, got %d\n",
			watches[1].key, watches[1].expop, watches[1].actualop)
	}

	//Set up a watch for deletion of a key

	err = kvh.Store(watches[2].key, watches[2].inival)
	if err != nil {
		t.Error("ERROR setting up a key:", watches[0].key, ":", err)
	}
	time.Sleep(500000 * time.Microsecond)

	cbh, err = kvh.WatchWithCB(watches[2].key, KVC_KEYCHANGE_DELETE,
		watch_cb_self_cancel, &watches[2])
	if err != nil {
		t.Error("ERROR setting up watch with callback[2]:", err)
	}

	//Sleep a tick, then delete the key

	time.Sleep(500000 * time.Microsecond)
	err = kvh.Delete(watches[2].key)
	if err != nil {
		t.Error("ERROR deleting key:", err)
	}
	time.Sleep(1 * time.Second)

	//Verify that it got caught.

	if !watches[2].hit {
		t.Errorf("ERROR, no watch was tripped for deleted key '%s'\n",
			watches[2].key)
	}
	if watches[2].expop != watches[2].actualop {
		t.Errorf("ERROR, watch trip for key '%s' unexpected operation, wanted %d, got %d\n",
			watches[2].key, watches[2].expop, watches[2].actualop)
	}

	err = kvh.Close()
	if err != nil {
		t.Error("ERROR closing memory-based KV interface:", err)
	}

	t.Logf("  ==> FINISHED WATCH WITH CALLBACK TEST **\n")
}
