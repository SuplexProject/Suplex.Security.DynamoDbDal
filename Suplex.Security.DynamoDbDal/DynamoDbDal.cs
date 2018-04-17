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
        public string TableName { get; set; }

        public Principal.User GetUserByUId(Guid userUId)
        {
            throw new NotImplementedException();
        }

        public List<Principal.User> GetUserByName(string name)
        {
            throw new NotImplementedException();
        }

        public Principal.User UpsertUser(Principal.User user)
        {
            if (user == null)
                return null;

            if (string.IsNullOrWhiteSpace(TableName))
                throw new Exception("Dynamo table name must be specified.");

            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient(clientConfig);

                JsonSerializerSettings settings = new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead
                };

                string output = JsonConvert.SerializeObject(user, Formatting.Indented, settings);
                Document doc = Document.FromJson(output);

                Table table = Table.LoadTable(client, TableName);
                if (table != null)
                {
                    table.PutItem(doc);
                }
                else
                {
                    throw new Exception($"Dynamo table {TableName} cannot be found.");
                }
            }
            catch (Exception ex)
            {
                Debug.Write(ex.Message);
                throw;
            }
            return user;
        }

        public void DeleteUser(Guid userUId)
        {
            throw new NotImplementedException();
        }

        public Principal.Group GetGroupByUId(Guid groupUId)
        {
            throw new NotImplementedException();
        }

        public List<Principal.Group> GetGroupByName(string name)
        {
            throw new NotImplementedException();
        }

        public Principal.Group UpsertGroup(Principal.Group @group)
        {
            throw new NotImplementedException();
        }

        public void DeleteGroup(Guid groupUId)
        {
            throw new NotImplementedException();
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
