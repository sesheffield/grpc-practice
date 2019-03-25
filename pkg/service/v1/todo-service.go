package v1

import (
	"context"
	"database/sql"

	"github.com/golang/protobuf/ptypes"
	v1 "github.com/sesheffield/grpc-practice/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// apiversion is version of API is provided by server
	apiVersion = "v1"
)

// toDoServiceServer is the implementation of v1.ToDoServiceServer proto interface
type toDoServiceServer struct {
	db *sql.DB
}

// NewToDoServiceServer creates ToDo service
func NewToDoServiceServer(db *sql.DB) v1.ToDoServiceServer {
	return &toDoServiceServer{
		db: db,
	}
}

// checkAPI checks if the API version requested by client is supported by server
func (s *toDoServiceServer) checkAPI(api string) error {
	// When API version is "" that means use current version of the service
	if len(api) > 0 {
		if apiVersion != api {
			return status.Errorf(codes.Unimplemented, "unsupported API version: service implements API version")
		}
	}
	return nil
}

func (s *toDoServiceServer) connect(ctx context.Context) (*sql.Conn, error) {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "failed to connected to database-> :%s", err.Error())
	}
	return c, nil
}

// Create new todo task
func (s *toDoServiceServer) Create(ctx context.Context, req *v1.CreateRequest) (*v1.CreateResponse, error) {
	// check if the API version requested by client is supported by server
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	// get SQL connection from pool
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	reminder, err := ptypes.Timestamp(req.ToDo.Reminder)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "reminder field has invalid format-> %s", err.Error())
	}
	query := "INSERT INTO ToDo(`Title`, `Description`, `Reminder`) VALUES(?, ?, ?)"
	// insert ToDo entity data
	res, err := c.ExecContext(ctx, query, req.ToDo.Title, req.ToDo.Description, reminder)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "failed to insert into ToDo-> %s", err.Error())
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "failed to retrieve id for created ToDo->%s", err.Error())
	}
	return &v1.CreateResponse{
		Api: apiVersion,
		Id:  id,
	}, nil
}

func (s *toDoServiceServer) Read(context.Context, *v1.ReadRequest) (*v1.ReadResponse, error) {
	panic("not implemented")
}

func (s *toDoServiceServer) Update(context.Context, *v1.UpdateResponse) (*v1.UpdateResponse, error) {
	panic("not implemented")
}

func (s *toDoServiceServer) Delete(context.Context, *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	panic("not implemented")
}

func (s *toDoServiceServer) ReadAll(context.Context, *v1.ReadAllRequest) (*v1.ReadAllResponse, error) {
	panic("not implemented")
}
