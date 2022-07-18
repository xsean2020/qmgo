package qmgo

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type GridFS struct {
	*gridfs.Bucket
	m sync.Mutex
}

type OpenGridfsOption struct {
	*options.BucketOptions
}

func GridfsOption() *OpenGridfsOption {
	return &OpenGridfsOption{BucketOptions: options.GridFSBucket()}
}

// Gridfs gets gridfs from database
func (d *Database) Gridfs(name string, opts ...*OpenGridfsOption) (*GridFS, error) {
	var mongoOpts []*options.BucketOptions
	mongoOpts = append(mongoOpts, options.GridFSBucket().SetName(name))
	for _, v := range opts {
		mongoOpts = append(mongoOpts, v.BucketOptions)
	}
	bo := options.MergeBucketOptions(mongoOpts...)
	bo.SetName(name)
	var gfs = new(GridFS)
	var err error
	gfs.Bucket, err = gridfs.NewBucket(d.database, bo)
	return gfs, err
}

func (gfs *GridFS) Create(name string) (file *GridFile) {
	fs := &GridFile{
		name:   name,
		GridFS: gfs,
	}
	return fs
}

type gfsDocId struct {
	Id interface{} "_id"
}

func (gfs *GridFS) Remove(ctx context.Context, name string) (err error) {
	gfs.m.Lock()
	defer gfs.m.Unlock()
	opt := options.Find()
	opt.SetProjection(bson.M{"_id": 1})
	cursor, err := gfs.GetFilesCollection().Find(ctx, bson.M{"filename": name}, opt)
	if err != nil {
		return err
	}

	c := Cursor{ctx: ctx, cursor: cursor}
	var doc gfsDocId
	defer c.Close()
	for c.Next(&doc) {
		err := gfs.RemoveId(doc.Id)
		if err != nil {
			return err
		}
	}
	return c.Err()
}

func (gfs *GridFS) RemoveId(id interface{}) error {
	return gfs.Bucket.Delete(id)
}

// func (gfs *GridFS) Find(query interface{}) *Query

// func (gfs *GridFS) OpenId(id interface{}) (file *GridFile, err error)
// func (gfs *GridFS) OpenNext(iter *Iter, file **GridFile) bool
// func (gfs *GridFS) Open(name string) (file *GridFile, err error) {

// }

// func (gfs *GridFS) Find(query interface{}) *Query

type GridFSFindOptions struct {
	*options.GridFSFindOptions
}

func (gfs *GridFS) Find(query interface{}, opts ...GridFSFindOptions) IQuery {
	return nil
}

func (gfs *GridFS) Open(name string) (file *GridFile, err error) {
	fs := &GridFile{
		name:   name,
		GridFS: gfs,
	}
	return fs, nil
}

func (gfs *GridFS) OpenId(id interface{}) (file *GridFile, err error) {
	fs := &GridFile{
		id:     id,
		GridFS: gfs,
	}
	return fs, nil
}

// func (gfs *GridFS) OpenId(id interface{}) (file *GridFile, err error) {}
// func (gfs *GridFS) OpenNext(iter *Iter, file **GridFile) bool         {}

type GridFile struct {
	*GridFS
	name string
	id   interface{}

	r *gridfs.DownloadStream
	w *gridfs.UploadStream
}

func (file *GridFile) Abort() {
	file.m.Lock()
	file.w.Abort()
	file.m.Unlock()
}

func (file *GridFile) Close() (err error) {
	file.m.Lock()
	defer file.m.Unlock()

	err = file.w.Close()
	if err != nil {
		return err
	}
	return file.r.Close()
}

func (file *GridFile) ContentType() string {

	// return file.Bucket.GetFilesCollection().Name()
}

func (file *GridFile) GetMeta(result interface{}) (err error) {

	return bson.Unmarshal(file.r.GetFile().Metadata, result)
}
func (file *GridFile) Id() interface{}
func (file *GridFile) MD5() (md5 string)
func (file *GridFile) Name() string
func (file *GridFile) Read(b []byte) (n int, err error)

func (file *GridFile) Seek(offset int64, whence int) (pos int64, err error)
func (file *GridFile) SetChunkSize(bytes int)
func (file *GridFile) SetContentType(ctype string)
func (file *GridFile) SetId(id interface{})
func (file *GridFile) SetMeta(metadata interface{})
func (file *GridFile) SetName(name string)
func (file *GridFile) SetUploadDate(t time.Time)
func (file *GridFile) Size() (bytes int64)
func (file *GridFile) UploadDate() time.Time
func (file *GridFile) Write(data []byte) (n int, err error) {

}
