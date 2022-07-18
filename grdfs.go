package qmgo

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Wrap mongo gridfs with bucket
type GridFS struct {
	*gridfs.Bucket
	m sync.Mutex
}

type OpenOption struct {
	*options.BucketOptions
}

type UploadOptions struct {
	*options.UploadOptions
}

type NameOptions struct {
	*options.NameOptions
}

type FindOptions struct {
	*options.GridFSFindOptions
}

func GridFSName() NameOptions {
	return NameOptions{options.GridFSName()}
}

func OpenGridfs() *OpenOption {
	return &OpenOption{BucketOptions: options.GridFSBucket()}
}

// Gridfs gets gridfs from database
func (d *Database) Gridfs(name string, opts ...*OpenOption) (*GridFS, error) {
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

// func (gfs *GridFS) Create(name string, opts ...*UploadOptions) (file *GridFile, err error) {
// 	file = new(GridFile)
// 	file.fs = gfs
// 	var mongoOpts []*options.UploadOptions
// 	for _, v := range opts {
// 		mongoOpts = append(mongoOpts, v.UploadOptions)
// 	}

// 	file.opt = options.MergeUploadOptions(mongoOpts...)
// 	file.f = &gridfs.File{Name: name, ID: primitive.NewObjectID()}

// 	gfs.OpenUploadStreamWithID()
// 	return
// }

// func (gfs *GridFS) Open(name string, opts ...NameOptions) (file *GridFile, err error) {
// 	var mongoOpts []*options.NameOptions
// 	for _, opt := range opts {
// 		mongoOpts = append(mongoOpts, opt.NameOptions)
// 	}
// 	opt := options.MergeNameOptions(mongoOpts...)

// 	file = new(GridFile)
// 	file.fs = gfs
// 	if file.r, err = gfs.OpenDownloadStreamByName(name, opt); err == nil {
// 		file.close = file.r.Close
// 	}
// 	return
// }

// func (gfs *GridFS) OpenId(id interface{}, options ...NameOptions) (file *GridFile, err error) {
// 	file = new(GridFile)
// 	file.fs = gfs
// 	if file.r, err = gfs.OpenDownloadStream(id); err == nil {
// 		file.close = file.r.Close
// 	}
// 	return
// }

type GridFSQuery struct {
	options.GridFSFindOptions
	gfs    *GridFS
	filter interface{}
	ctx    context.Context
}

// func (query *GridFSQuery) All() error {

// }

// func (query *GridFSQuery)

func (gfs *GridFS) Find(ctx context.Context, filter interface{}, opts ...GridFSFindOptions) *GridFSQuery {
	query := new(GridFSQuery)
	query.filter = filter
	query.gfs = gfs
	query.ctx = ctx
	return query
}

// func (gfs *GridFS) OpenNext(iter *Iter, file **GridFile) bool {

// 	return false
// }

// func (gfs *GridFS) Find(query interface{}) *Query

type GridFSFindOptions struct {
	*options.GridFSFindOptions
}

// type GridFile struct {
// 	fs *GridFS
// 	m  sync.Mutex
// 	// r  *gridfs.DownloadStream
// 	// w  *gridfs.UploadStream

// 	opt *options.UploadOptions
// 	// f     *gridfs.File
// 	close func() error
// 	read  func(p byte) int
// 	write func(p byte) int
// }

// func (file *GridFile) Abort() {
// 	if file.w != nil {
// 		file.w.Abort()
// 	}
// }

// func (file *GridFile) Close() (err error) {
// 	file.m.Lock()
// 	defer file.m.Unlock()
// 	if file.close == nil {
// 		return nil
// 	}
// 	return file.close()
// }

// // func (file *GridFile) ContentType() string {

// // 	// return file.Bucket.GetFilesCollection().Name()
// // }

// func (file *GridFile) GetMeta(result interface{}) (err error) {
// 	return bson.Unmarshal(file.f.Metadata, result)
// }

// func (file *GridFile) Id() interface{} {
// 	return file.f.ID
// }
// func (file *GridFile) MD5() (md5 string)
// func (file *GridFile) Name() string {
// 	return file.f.Name
// }
// func (file *GridFile) Read(b []byte) (n int, err error) {
// 	return file.r.Read(b)
// }

// // func (file *GridFile) Seek(offset int64, whence int) (pos int64, err error) {

// // 	// return file.opt.ChunkSizeBytes
// // }
// // func (file *GridFile) SetChunkSize(bytes int) {
// // 	file.opt.SetChunkSizeBytes(int32(bytes))
// // }
// // func (file *GridFile) SetContentType(ctype string) {
// // 	// file.opt.Set
// // }
// func (file *GridFile) SetId(id interface{}) {
// 	file.f.ID = id
// }
// func (file *GridFile) SetMeta(metadata interface{}) {
// 	file.opt.SetMetadata(metadata)
// }

func (file *GridFile) SetName(name string) { // close 的是否改变
	// file.f.Name = name

}

// func (file *GridFile) SetUploadDate(t time.Time)

// func (file *GridFile) Size() (bytes int64) {
// 	return file.f.Length
// }
// func (file *GridFile) UploadDate() time.Time {
// 	return file.f.UploadDate
// }

// func (file *GridFile) Write(data []byte) (n int, err error) {
// 	file.m.Lock()
// 	defer file.m.Unlock()
// 	return file.w.Write(data)
// }
