# -*- encoding : utf-8 -*-
require_relative '../spec_helper'

describe Cequel::Metal::Keyspace do
  before :all do
    cequel.schema.create_table(:posts) do
      key :id, :int
      column :title, :text
      column :body, :text
    end
  end

  after :each do
    ids = cequel[:posts].select(:id).map { |row| row[:id] }
    cequel[:posts].where(id: ids).delete if ids.any?
  end

  after :all do
    cequel.schema.drop_table(:posts)
  end

  describe '::batch' do
    it 'should send enclosed write statements in bulk' do
      expect_statement_count 1 do
        cequel.batch do
          cequel[:posts].insert(id: 1, title: 'Hey')
          cequel[:posts].where(id: 1).update(body: 'Body')
          cequel[:posts].where(id: 1).delete(:title)
        end
      end
      expect(cequel[:posts].first).to eq({id: 1, title: nil, body: 'Body'}
        .with_indifferent_access)
    end

    it 'should auto-apply if option given' do
      cequel.batch(auto_apply: 2) do
        cequel[:posts].insert(id: 1, title: 'One')
        expect(cequel[:posts].count).to be_zero
        cequel[:posts].insert(id: 2, title: 'Two')
        expect(cequel[:posts].count).to be(2)
      end
    end

    it 'should do nothing if no statements executed in batch' do
      expect { cequel.batch {} }.to_not raise_error
    end

    it 'should execute unlogged batch if specified' do
      expect_query_with_consistency(/BEGIN UNLOGGED BATCH/, anything) do
        cequel.batch(unlogged: true) do
          cequel[:posts].insert(id: 1, title: 'One')
          cequel[:posts].insert(id: 2, title: 'Two')
        end
      end
    end

    it 'should execute batch with given consistency' do
      expect_query_with_consistency(/BEGIN BATCH/, :one) do
        cequel.batch(consistency: :one) do
          cequel[:posts].insert(id: 1, title: 'One')
          cequel[:posts].insert(id: 2, title: 'Two')
        end
      end
    end

    it 'should raise error if consistency specified in individual query in batch' do
      expect {
        cequel.batch(consistency: :one) do
          cequel[:posts].consistency(:quorum).insert(id: 1, title: 'One')
        end
      }.to raise_error(ArgumentError)
    end
  end

  describe "#exists?" do
    it "is true for existent keyspaces" do
      expect(cequel.exists?).to eq true
    end

    it "is false for non-existent keyspaces" do
      nonexistent_keyspace = Cequel.connect host: Cequel::SpecSupport::Helpers.host,
                           port: Cequel::SpecSupport::Helpers.port,
                           keyspace: "totallymadeup"

      expect(nonexistent_keyspace.exists?).to be false
    end
  end

  describe "#ssl_config" do
    it "ssl configuration settings get extracted correctly for sending to cluster" do
      connect = Cequel.connect host: Cequel::SpecSupport::Helpers.host,
                           port: Cequel::SpecSupport::Helpers.port,
                           ssl: true,
                           server_cert: 'path/to/server_cert',
                           client_cert: 'path/to/client_cert',
                           private_key: 'private_key',
                           passphrase: 'passphrase'

      expect(connect.ssl_config[:ssl]).to be true
      expect(connect.ssl_config[:server_cert]).to eq('path/to/server_cert')
      expect(connect.ssl_config[:client_cert]).to eq('path/to/client_cert')
      expect(connect.ssl_config[:private_key]).to eq('private_key')
      expect(connect.ssl_config[:passphrase]).to eq('passphrase')
    end
  end

  shared_context 'with fake cluster instance' do
    let(:cluster) { double(:cluster) }

    before(:each) do
      allow(cluster).to receive(:connect).with(anything)
    end
  end

  describe '#datacenter' do
    include_context 'with fake cluster instance'

    let(:connection) { Cequel.connect(connection_options) }
    subject(:return_value) { connection.datacenter }

    context 'with datacenter set' do
      let(:datacenter) { 'current_datacenter' }
      let(:connection_options) do
        {
          host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port,
          datacenter: datacenter
        }
      end

      it 'returns the datacenter setting for the cluster connection' do
        expect(connection.datacenter).to eq(datacenter)
      end

      describe 'client instantiation' do
        subject(:client) { connection.client }

        it 'passes datacenter to Cassandra.cluster' do
          expect(Cassandra).to receive(:cluster) do |options|
            expect(options).to include(:datacenter)
            expect(options[:datacenter]).to eq datacenter
            cluster
          end

          client
        end
      end
    end

    context 'without datacenter set' do
      let(:connection_options) do
        {
          host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port
        }
      end

      it 'defaults to nil' do
        expect(connection.datacenter).to be nil
      end

      describe 'client instantiation' do
        subject(:client) { connection.client }

        it "doesn't pass datacenter to Cassandra.cluster" do
          expect(Cassandra).to receive(:cluster) do |options|
            expect(options).not_to include(:datacenter)
            cluster
          end

          client
        end
      end
    end
  end

  describe '#connections_per_remote_node' do
    include_context 'with fake cluster instance'

    let(:connection) { Cequel.connect(connection_options) }
    subject(:return_value) { connection.connnections_per_remote_node }

    context 'with connections_per_remote_node set' do
      let(:connection_options) do
        {
          host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port,
          connections_per_remote_node: 0
        }
      end

      it 'returns the connections_per_remote_node setting for the cluster connection' do
        expect(connection.connections_per_remote_node).to eq 0
      end

      describe 'client instantiation' do
        subject(:client) { connection.client }

        it 'passes connections_per_remote_node to Cassandra.cluster' do
          expect(Cassandra).to receive(:cluster) do |options|
            expect(options).to include(:connections_per_remote_node)
            expect(options[:connections_per_remote_node]).to eq 0
            cluster
          end

          client
        end
      end
    end

    context 'without connections_per_remote_node set' do
      let(:connection_options) do
        {
          host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port
        }
      end

      it 'defaults to nil' do
        expect(connection.connections_per_remote_node).to be nil
      end

      describe 'client instantiation' do
        subject(:client) { connection.client }

        it 'does not pass connections_per_remote_node to Cassandra.cluster' do
          expect(Cassandra).to receive(:cluster) do |options|
            expect(options).to_not include(:connections_per_remote_node)
            cluster
          end

          client
        end
      end
    end
  end

  describe '#load_balancing_policy' do
    include_context 'with fake cluster instance'

    let(:connection) { Cequel.connect(connection_options) }
    subject(:return_value) { connection.load_balancing_policy }

    context 'with load_balancing_policy set' do
      let(:load_balancing_policy) { double(:load_balancing_policy) }
      let(:connection_options) do
        {
          host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port,
          load_balancing_policy: load_balancing_policy
        }
      end

      it { is_expected.to be load_balancing_policy }

      describe 'client instantiation' do
        subject(:client) { connection.client }

        it 'passes load_balancing_policy to Cassandra.cluster' do
          expect(Cassandra).to receive(:cluster) do |options|
            expect(options).to include(:load_balancing_policy)
            expect(options[:load_balancing_policy]).to be load_balancing_policy
            cluster
          end

          client
        end
      end
    end

    context 'without a load_balancing_policy set' do
      let(:connection_options) do
        {
          host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port,
        }
      end

      it { is_expected.to be nil }

      describe 'client instantiation' do
        subject(:client) { connection.client }

        it 'does not pass load_balancing_policy to Cassandra.cluster' do
          expect(Cassandra).to receive(:cluster) do |options|
            expect(options).not_to include(:load_balancing_policy)
            cluster
          end

          client
        end
      end
    end
  end

  describe "#execute" do
    let(:statement) { "SELECT id FROM posts" }

    context "without a connection error" do
      it "executes a CQL query" do
        expect { cequel.execute(statement) }.not_to raise_error
      end
    end

    context "with a connection error" do
      it "reconnects to cassandra with a new client after first failed connection" do
        allow(cequel.client).to receive(:execute)
          .with(statement, :consistency => cequel.default_consistency)
          .and_raise(Ione::Io::ConnectionError)
          .once

        expect { cequel.execute(statement) }.not_to raise_error
      end
    end
  end
end
