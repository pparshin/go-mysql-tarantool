package bridge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_makeInsertRequest(t *testing.T) {
	type args struct {
		r   *rule
		row []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    *request
		wantErr bool
	}{
		{
			name: "SinglePK",
			args: args{
				r: &rule{
					schema: "city",
					table:  "users",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "id",
							unsigned: true,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "name",
							unsigned: false,
						},
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "password",
							unsigned: false,
						},
					},
					space: "users",
				},
				row: []interface{}{1, "bob", "12345"},
			},
			want: &request{
				action: actionInsert,
				space:  "users",
				keys: []reqArg{
					{
						field: 0,
						value: uint64(1),
					},
				},
				args: []reqArg{
					{
						field: 1,
						value: "bob",
					},
					{
						field: 2,
						value: "12345",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "MultiplePK",
			args: args{
				r: &rule{
					schema: "city",
					table:  "logins",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "user_id",
							unsigned: true,
						},
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "user_ip",
							unsigned: false,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "attempts",
							unsigned: false,
						},
					},
					space: "logins",
				},
				row: []interface{}{1, "10.20.10.1", uint64(5)},
			},
			want: &request{
				action: actionInsert,
				space:  "logins",
				keys: []reqArg{
					{
						field: 0,
						value: uint64(1),
					},
					{
						field: 1,
						value: "10.20.10.1",
					},
				},
				args: []reqArg{
					{
						field: 2,
						value: uint64(5),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "InvalidRow",
			args: args{
				r: &rule{
					schema: "city",
					table:  "users",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "id",
							unsigned: true,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "name",
							unsigned: false,
						},
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "password",
							unsigned: false,
						},
					},
					space: "users",
				},
				row: []interface{}{1, "bob"},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeInsertRequest(tt.args.r, tt.args.row)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.EqualValues(t, tt.want, got)
			}
		})
	}
}

func Test_makeUpdateRequests(t *testing.T) {
	type args struct {
		r    *rule
		rows [][]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []*request
		wantErr bool
	}{
		{
			name: "SinglePK_UpdateOnlyArgs",
			args: args{
				r: &rule{
					schema: "city",
					table:  "users",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "id",
							unsigned: true,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "name",
							unsigned: false,
						},
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "password",
							unsigned: false,
						},
					},
					space: "users",
				},
				rows: [][]interface{}{
					{1, "bob", "12345"},
					{1, "bob", "qwerty"},
				},
			},
			want: []*request{
				{
					action: actionUpdate,
					space:  "users",
					keys: []reqArg{
						{
							field: 0,
							value: uint64(1),
						},
					},
					args: []reqArg{
						{
							field: 1,
							value: "bob",
						},
						{
							field: 2,
							value: "qwerty",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "SinglePK_UpdatePK",
			args: args{
				r: &rule{
					schema: "city",
					table:  "users",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "id",
							unsigned: true,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "name",
							unsigned: false,
						},
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "password",
							unsigned: false,
						},
					},
					space: "users",
				},
				rows: [][]interface{}{
					{1, "bob", "12345"},
					{2, "bob", "qwerty"},
				},
			},
			want: []*request{
				{
					action: actionDelete,
					space:  "users",
					keys: []reqArg{
						{
							field: 0,
							value: uint64(1),
						},
					},
				},
				{
					action: actionInsert,
					space:  "users",
					keys: []reqArg{
						{
							field: 0,
							value: uint64(2),
						},
					},
					args: []reqArg{
						{
							field: 1,
							value: "bob",
						},
						{
							field: 2,
							value: "qwerty",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "MultiplePK_UpdateOnlyArgs",
			args: args{
				r: &rule{
					schema: "city",
					table:  "logins",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "user_id",
							unsigned: true,
						},
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "user_ip",
							unsigned: false,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "attempts",
							unsigned: false,
						},
					},
					space: "logins",
				},
				rows: [][]interface{}{
					{1, "10.20.10.1", uint64(1)},
					{1, "10.20.10.1", uint64(2)},
				},
			},
			want: []*request{
				{
					action: actionUpdate,
					space:  "logins",
					keys: []reqArg{
						{
							field: 0,
							value: uint64(1),
						},
						{
							field: 1,
							value: "10.20.10.1",
						},
					},
					args: []reqArg{
						{
							field: 2,
							value: uint64(2),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "InvalidRows",
			args: args{
				r: &rule{
					schema: "city",
					table:  "users",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "id",
							unsigned: true,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "name",
							unsigned: false,
						},
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "password",
							unsigned: false,
						},
					},
					space: "users",
				},
				rows: [][]interface{}{
					{1, "bob", "12345"},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeUpdateRequests(tt.args.r, tt.args.rows)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.EqualValues(t, tt.want, got)
			}
		})
	}
}

func Test_makeDeleteRequest(t *testing.T) {
	type args struct {
		r   *rule
		row []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    *request
		wantErr bool
	}{
		{
			name: "SinglePK",
			args: args{
				r: &rule{
					schema: "city",
					table:  "users",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "id",
							unsigned: true,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "name",
							unsigned: false,
						},
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "password",
							unsigned: false,
						},
					},
					space: "users",
				},
				row: []interface{}{1, "bob", "12345"},
			},
			want: &request{
				action: actionDelete,
				space:  "users",
				keys: []reqArg{
					{
						field: 0,
						value: uint64(1),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "MultiplePK",
			args: args{
				r: &rule{
					schema: "city",
					table:  "logins",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "user_id",
							unsigned: true,
						},
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "user_ip",
							unsigned: false,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "attempts",
							unsigned: false,
						},
					},
					space: "logins",
				},
				row: []interface{}{1, "10.20.10.1", uint64(5)},
			},
			want: &request{
				action: actionDelete,
				space:  "logins",
				keys: []reqArg{
					{
						field: 0,
						value: uint64(1),
					},
					{
						field: 1,
						value: "10.20.10.1",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "InvalidRow",
			args: args{
				r: &rule{
					schema: "city",
					table:  "users",
					pks: []*attribute{
						{
							colIndex: 0,
							tupIndex: 0,
							name:     "id",
							unsigned: true,
						},
					},
					attrs: []*attribute{
						{
							colIndex: 1,
							tupIndex: 1,
							name:     "name",
							unsigned: false,
						},
						{
							colIndex: 2,
							tupIndex: 2,
							name:     "password",
							unsigned: false,
						},
					},
					space: "users",
				},
				row: []interface{}{},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeDeleteRequest(tt.args.r, tt.args.row)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.EqualValues(t, tt.want, got)
			}
		})
	}
}
