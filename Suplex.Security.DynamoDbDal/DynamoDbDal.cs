using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Suplex.Security.AclModel;
using Suplex.Security.AclModel.DataAccess;
using Suplex.Security.Principal;

namespace Suplex.Security.DynamoDbDal
{
    public class DynamoDbDal : IDataAccessLayer
    {
        public string UserTable { get; set; }
        public string GroupTable { get; set; }

        public User GetUserByUId(Guid userUId)
        {
            User user;

            if ( userUId == null || userUId == Guid.Empty )
                throw new Exception( "User unique Id cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( UserTable ) )
                throw new Exception( "User table name must be specified." );

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient( clientConfig );

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                Table table = Table.LoadTable( client, UserTable );
                if ( table != null )
                {
                    Document document = table.GetItem( userUId );
                    string json = document.ToJsonPretty();
                    Console.WriteLine( json );
                    user = JsonConvert.DeserializeObject<User>( json, settings );
                }
                else
                {
                    throw new Exception( $"Table {UserTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return user;
        }

        public List<User> GetUserByName(string name)
        {
            List<User> userList = new List<User>();
            if ( string.IsNullOrWhiteSpace( name ) )
                throw new Exception( "User's name must be specified." );

            if ( string.IsNullOrWhiteSpace( UserTable ) )
                throw new Exception( "User table name must be specified." );

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient( clientConfig );

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                Table table = Table.LoadTable( client, UserTable );
                if ( table != null )
                {
                    ScanFilter scanFilter = new ScanFilter();
                    scanFilter.AddCondition( "Name", ScanOperator.Equal, "User.70a7a4b6-9326-4cb2-a073-3ffe5379ad2f" );

                    Search search = table.Scan( scanFilter );

                    do
                    {
                        var documentList = search.GetNextSet();
                        foreach ( Document document in documentList )
                        {
                            string json = document.ToJsonPretty();
                            User user = JsonConvert.DeserializeObject<User>( json, settings );
                            userList.Add( user );
                        }
                    } while ( !search.IsDone );
                }
                else
                {
                    throw new Exception( $"Dynamo table {UserTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }

            return userList;
        }

        public Principal.User UpsertUser(Principal.User user)
        {
            if ( user == null )
                return null;

            if ( string.IsNullOrWhiteSpace( UserTable ) )
                throw new Exception( "User table name must be specified." );

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient( clientConfig );

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                string output = JsonConvert.SerializeObject( user, Formatting.Indented, settings );
                Document doc = Document.FromJson( output );

                Table table = Table.LoadTable( client, UserTable );
                if ( table != null )
                {
                    table.PutItem( doc );
                }
                else
                {
                    throw new Exception( $"Dynamo table {UserTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return user;
        }

        public void DeleteUser(Guid userUId)
        {
            if ( userUId == null || userUId == Guid.Empty )
                throw new Exception( "User unique Id cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( UserTable ) )
                throw new Exception( "User table name must be specified." );

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient( clientConfig );

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                Table table = Table.LoadTable( client, UserTable );
                if ( table != null )
                {
                    table.DeleteItem( userUId );
                    Document deletedUser = table.GetItem( (userUId), new GetItemOperationConfig()
                    {
                        ConsistentRead = true
                    } );
                    if ( deletedUser != null )
                        throw new Exception( "User was not deleted successfully." );
                }
                else
                {
                    throw new Exception( $"Table {UserTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
        }

        public Group GetGroupByUId(Guid groupUId)
        {
            Group group;

            if ( groupUId == null || groupUId == Guid.Empty )
                throw new Exception( "Group unique Id cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( GroupTable ) )
                throw new Exception( "Group table name must be specified." );

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient( clientConfig );

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                Table table = Table.LoadTable( client, GroupTable );
                if ( table != null )
                {
                    Document document = table.GetItem( groupUId );
                    string json = document.ToJsonPretty();
                    Console.WriteLine( json );
                    group = JsonConvert.DeserializeObject<Group>( json, settings );
                }
                else
                {
                    throw new Exception( $"Table {GroupTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return group;
        }

        public List<Group> GetGroupByName(string name)
        {
            List<Group> groupList = new List<Group>();
            if ( string.IsNullOrWhiteSpace( name ) )
                throw new Exception( "Group's name must be specified." );

            if ( string.IsNullOrWhiteSpace( GroupTable ) )
                throw new Exception( "Group table name must be specified." );

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient( clientConfig );

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                Table table = Table.LoadTable( client, GroupTable );
                if ( table != null )
                {
                    ScanFilter scanFilter = new ScanFilter();
                    scanFilter.AddCondition( "Name", ScanOperator.Equal, "Group.70a7a4b6-9326-4cb2-a073-3ffe5379ad2f" );

                    Search search = table.Scan( scanFilter );

                    do
                    {
                        var documentList = search.GetNextSet();
                        foreach ( Document document in documentList )
                        {
                            string json = document.ToJsonPretty();
                            Group group = JsonConvert.DeserializeObject<Group>( json, settings );
                            groupList.Add( group );
                        }
                    } while ( !search.IsDone );
                }
                else
                {
                    throw new Exception( $"Dynamo table {GroupTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }

            return groupList;
        }

        public Group UpsertGroup(Group group)
        {
            if ( group == null )
                return null;

            if ( string.IsNullOrWhiteSpace( GroupTable ) )
                throw new Exception( "Group table name must be specified." );

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient( clientConfig );

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                string output = JsonConvert.SerializeObject( group, Formatting.Indented, settings );
                Document doc = Document.FromJson( output );

                Table table = Table.LoadTable( client, GroupTable );
                if ( table != null )
                {
                    table.PutItem( doc );
                }
                else
                {
                    throw new Exception( $"Dynamo table {GroupTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return group;
        }

        public void DeleteGroup(Guid groupUId)
        {
            if ( groupUId == null || groupUId == Guid.Empty )
                throw new Exception( "Group unique Id cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( GroupTable ) )
                throw new Exception( "Group table name must be specified." );

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient( clientConfig );

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                Table table = Table.LoadTable( client, GroupTable );
                if ( table != null )
                {
                    table.DeleteItem( groupUId );
                    Document deletedGroup = table.GetItem( (groupUId), new GetItemOperationConfig()
                    {
                        ConsistentRead = true
                    } );
                    if ( deletedGroup != null )
                        throw new Exception( "Group was not deleted successfully." );
                }
                else
                {
                    throw new Exception( $"Table {GroupTable} cannot be found." );
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
        }

        public IEnumerable<GroupMembershipItem> GetGroupMembers(Guid groupUId, bool includeDisabledMembership = false)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<GroupMembershipItem> GetGroupMembership(Guid memberUId, bool includeDisabledMembership = false)
        {
            throw new NotImplementedException();
        }

        public GroupMembershipItem UpsertGroupMembership(GroupMembershipItem groupMembershipItem)
        {
            throw new NotImplementedException();
        }

        public void DeleteGroupMembership(GroupMembershipItem groupMembershipItem)
        {
            throw new NotImplementedException();
        }

        public ISecureObject GetSecureObjectByUId(Guid secureObjectUId, bool includeChildren)
        {
            throw new NotImplementedException();
        }

        public ISecureObject GetSecureObjectByUniqueName(string uniqueName, bool includeChildren)
        {
            throw new NotImplementedException();
        }

        public ISecureObject UpsertSecureObject(ISecureObject secureObject)
        {
            throw new NotImplementedException();
        }

        public void DeleteSecureObject(Guid secureObjectUId)
        {
            throw new NotImplementedException();
        }
    }
}
