﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Amazon.DynamoDBv2;
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

        public string GroupMembershipTable { get; set; }

        public string SecureObjectTable { get; set; }

        private readonly AmazonDynamoDBConfig _clientConfig;
        private readonly AmazonDynamoDBClient _client;
        private readonly JsonSerializerSettings _settings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.Objects,
            MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
        };

        public DynamoDbDal()
        {
            _clientConfig = new AmazonDynamoDBConfig();
            _client = new AmazonDynamoDBClient( _clientConfig );
        }

        public DynamoDbDal(AmazonDynamoDBClient client, AmazonDynamoDBConfig clientConfig)
        {
            _client = client;
            _clientConfig = clientConfig;
        }

        public User GetUserByUId(Guid userUId)
        {
            User user;

            if ( userUId == Guid.Empty )
                throw new Exception( "User unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( UserTable ) )
                throw new Exception( "User table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, UserTable );
                Document document = table.GetItem( userUId );
                if ( document == null )
                    throw new Exception( "User cannot be found." );

                string json = document.ToJsonPretty();
                Console.WriteLine( json );
                user = JsonConvert.DeserializeObject<User>( json, _settings );
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
                throw new Exception( "User's name cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( UserTable ) )
                throw new Exception( "User table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, UserTable );
                if ( table != null )
                {
                    ScanFilter scanFilter = new ScanFilter();
                    scanFilter.AddCondition( "Name", ScanOperator.Equal, name );

                    Search search = table.Scan( scanFilter );

                    do
                    {
                        List<Document> documentList = search.GetNextSet();
                        if ( documentList.Count == 0 )
                            throw new Exception( "User cannot be found." );

                        foreach ( Document document in documentList )
                        {
                            string json = document.ToJsonPretty();
                            User user = JsonConvert.DeserializeObject<User>( json, _settings );
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

        public User UpsertUser(User user)
        {
            if ( user == null )
                throw new Exception( "User cannot be null." );

            if ( string.IsNullOrWhiteSpace( UserTable ) )
                throw new Exception( "User table name must be specified." );

            try
            {
                string output = JsonConvert.SerializeObject( user, Formatting.Indented, _settings );
                Document doc = Document.FromJson( output );

                Table table = Table.LoadTable( _client, UserTable );
                table.PutItem( doc );
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
                throw new Exception( "User unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( UserTable ) )
                throw new Exception( "User table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, UserTable );
                if ( table != null )
                {
                    table.DeleteItem( userUId );
                    Document residual = table.GetItem( userUId, new GetItemOperationConfig()
                    {
                        ConsistentRead = true
                    } );
                    if ( residual != null )
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
                throw new Exception( "Group unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( GroupTable ) )
                throw new Exception( "Group table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, GroupTable );
                Document document = table.GetItem( groupUId );
                if ( document == null )
                    throw new Exception( "Group cannot be found." );
                string json = document.ToJsonPretty();
                Console.WriteLine( json );
                group = JsonConvert.DeserializeObject<Group>( json, _settings );
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
                Table table = Table.LoadTable( _client, GroupTable );
                if ( table != null )
                {
                    ScanFilter scanFilter = new ScanFilter();
                    scanFilter.AddCondition( "Name", ScanOperator.Equal, name );

                    Search search = table.Scan( scanFilter );
                    do
                    {
                        var documentList = search.GetNextSet();
                        if ( documentList.Count == 0 )
                            throw new Exception( "Group cannot be found." );
                        foreach ( Document document in documentList )
                        {
                            string json = document.ToJsonPretty();
                            Group group = JsonConvert.DeserializeObject<Group>( json, _settings );
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
                throw new Exception( "Group cannot be null." );

            if ( string.IsNullOrWhiteSpace( GroupTable ) )
                throw new Exception( "Group table name must be specified." );

            try
            {
                string output = JsonConvert.SerializeObject( group, Formatting.Indented, _settings );
                Document doc = Document.FromJson( output );

                Table table = Table.LoadTable( _client, GroupTable );
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
                throw new Exception( "Group unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( GroupTable ) )
                throw new Exception( "Group table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, GroupTable );
                if ( table != null )
                {
                    table.DeleteItem( groupUId );
                    Document deletedGroup = table.GetItem( groupUId, new GetItemOperationConfig()
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

            List<GroupMembershipItem> groupMembershipList = new List<GroupMembershipItem>();

            if ( groupUId == Guid.Empty )
                throw new Exception( "Group unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( GroupMembershipTable ) )
                throw new Exception( "Group membership table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, GroupMembershipTable );
                ScanFilter scanFilter = new ScanFilter();
                scanFilter.AddCondition( "GroupUId", ScanOperator.Equal, groupUId );

                Search search = table.Scan( scanFilter );

                do
                {
                    var documentList = search.GetNextSet();
                    if ( documentList.Count == 0 )
                        throw new Exception( "Group members cannot be found." );

                    foreach ( Document document in documentList )
                    {
                        string json = document.ToJsonPretty();
                        GroupMembershipItem item = JsonConvert.DeserializeObject<GroupMembershipItem>( json, _settings );
                        if ( includeDisabledMembership )
                        {
                            groupMembershipList.Add( item );
                        }
                        else if ( item.Member != null && item.Member.IsEnabled )
                        {
                            groupMembershipList.Add( item );
                        }
                    }
                } while ( !search.IsDone );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return groupMembershipList;
        }

        public IEnumerable<GroupMembershipItem> GetGroupMembership(Guid memberUId, bool includeDisabledMembership = false)
        {
            List<GroupMembershipItem> groupMembershipList = new List<GroupMembershipItem>();

            if ( memberUId == Guid.Empty )
                throw new Exception( "Member unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( GroupMembershipTable ) )
                throw new Exception( "Group membership table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, GroupMembershipTable );
                ScanFilter scanFilter = new ScanFilter();
                scanFilter.AddCondition( "MemberUId", ScanOperator.Equal, memberUId );

                Search search = table.Scan( scanFilter );

                do
                {
                    var documentList = search.GetNextSet();
                    if ( documentList.Count == 0 )
                        throw new Exception( "Group members cannot be found." );
                    foreach ( Document document in documentList )
                    {
                        string json = document.ToJsonPretty();
                        GroupMembershipItem item = JsonConvert.DeserializeObject<GroupMembershipItem>( json, _settings );
                        if ( includeDisabledMembership )
                        {
                            groupMembershipList.Add( item );
                        }
                        else if ( item.Member != null && item.Member.IsEnabled )
                        {
                            groupMembershipList.Add( item );
                        }
                    }
                } while ( !search.IsDone );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return groupMembershipList;
        }

        public GroupMembershipItem UpsertGroupMembership(GroupMembershipItem groupMembershipItem)
        {
            if ( groupMembershipItem == null )
                throw new Exception( "Group membership cannot be null." );

            if ( groupMembershipItem.GroupUId == Guid.Empty )
                throw new Exception( "Group unique id cannot be empty." );

            if ( groupMembershipItem.MemberUId == Guid.Empty )
                throw new Exception( "Member unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( GroupMembershipTable ) )
                throw new Exception( "Group membership table name must be specified." );

            try
            {
                string output = JsonConvert.SerializeObject( groupMembershipItem, Formatting.Indented, _settings );
                Document doc = Document.FromJson( output );

                Table table = Table.LoadTable( _client, GroupMembershipTable );
                table.PutItem( doc );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return groupMembershipItem;
        }

        public void DeleteGroupMembership(GroupMembershipItem groupMembershipItem)
        {

            if ( groupMembershipItem == null )
                throw new Exception( "Group membership cannot be null." );

            if ( groupMembershipItem.GroupUId == Guid.Empty )
                throw new Exception( "Group unique id cannot be empty." );

            if ( groupMembershipItem.MemberUId == Guid.Empty )
                throw new Exception( "Member unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( GroupMembershipTable ) )
                throw new Exception( "Group membership table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, GroupMembershipTable );
                table.DeleteItem( groupMembershipItem.GroupUId, groupMembershipItem.MemberUId );
                Document deletedGroup = table.GetItem( groupMembershipItem.GroupUId, groupMembershipItem.MemberUId, new GetItemOperationConfig()
                {
                    ConsistentRead = true
                } );
                if ( deletedGroup != null )
                    throw new Exception( "Group membership item was not deleted successfully." );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
        }

        public ISecureObject GetSecureObjectByUId(Guid secureObjectUId, bool includeChildren)
        {
            SecureObject secureObject;

            if ( secureObjectUId == Guid.Empty )
                throw new Exception( "SecureObject unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( SecureObjectTable ) )
                throw new Exception( "SecureObject table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, SecureObjectTable );
                Document document = table.GetItem( secureObjectUId );
                if ( document == null )
                    throw new Exception( "SecureObject cannot be found." );
                string json = document.ToJsonPretty();
                Console.WriteLine( json );
                secureObject = JsonConvert.DeserializeObject<SecureObject>( json, _settings );
                if ( secureObject != null && !includeChildren )
                    secureObject.Children = null;
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return secureObject;
        }

        public ISecureObject GetSecureObjectByUniqueName(string uniqueName, bool includeChildren)
        {
            SecureObject secureObject = null;

            List<SecureObject> secureObjectList = new List<SecureObject>();

            if ( string.IsNullOrWhiteSpace( uniqueName ) )
                throw new Exception( "SecureObject unique name cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( SecureObjectTable ) )
                throw new Exception( "SecureObject table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, SecureObjectTable );
                ScanFilter scanFilter = new ScanFilter();
                scanFilter.AddCondition( "UniqueName", ScanOperator.Equal, uniqueName );

                Search search = table.Scan( scanFilter );

                do
                {
                    var documentList = search.GetNextSet();
                    if ( documentList.Count == 0 )
                        throw new Exception( "SecureObject cannot be found." );
                    foreach ( Document document in documentList )
                    {
                        string json = document.ToJsonPretty();
                        SecureObject sb = JsonConvert.DeserializeObject<SecureObject>( json, _settings );
                        secureObjectList.Add( sb );
                    }
                } while ( !search.IsDone );

                if ( secureObjectList.Count > 0 )
                {
                    secureObject = secureObjectList.First();
                    if ( !includeChildren )
                        secureObject.Children = null;
                }
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return secureObject;
        }

        public ISecureObject UpsertSecureObject(ISecureObject secureObject)
        {
            if ( secureObject == null )
                throw new Exception( "SecureObject cannot be null." );

            if ( secureObject.UId == Guid.Empty )
                throw new Exception( "SecureObject unique id cannot be null." );


            if ( string.IsNullOrWhiteSpace( secureObject.UniqueName ) )
                throw new Exception( "SecureObject unique name cannot be null or empty." );

            if ( string.IsNullOrWhiteSpace( SecureObjectTable ) )
                throw new Exception( "SecureObject table name must be specified." );

            try
            {
                string output = JsonConvert.SerializeObject( secureObject, Formatting.Indented, _settings );
                Document doc = Document.FromJson( output );

                Table table = Table.LoadTable( _client, SecureObjectTable );
                table.PutItem( doc );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
            return secureObject;

        }

        public void DeleteSecureObject(Guid secureObjectUId)
        {
            if ( secureObjectUId == Guid.Empty )
                throw new Exception( "SecureObject unique id cannot be empty." );

            if ( string.IsNullOrWhiteSpace( SecureObjectTable ) )
                throw new Exception( "SecureObject table name must be specified." );

            try
            {
                Table table = Table.LoadTable( _client, SecureObjectTable );
                table.DeleteItem( secureObjectUId );
                Document deletedGroup = table.GetItem( secureObjectUId, new GetItemOperationConfig()
                {
                    ConsistentRead = true
                } );
                if ( deletedGroup != null )
                    throw new Exception( "Secure object was not deleted successfully." );
            }
            catch ( Exception ex )
            {
                Debug.Write( ex.Message );
                throw;
            }
        }
    }
}
