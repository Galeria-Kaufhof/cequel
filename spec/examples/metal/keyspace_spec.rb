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

  describe "#datacenter" do
    it "datacenter setting get extracted correctly for sending to cluster" do
      connect = Cequel.connect host: Cequel::SpecSupport::Helpers.host,
                           port: Cequel::SpecSupport::Helpers.port,
                           datacenter: 'current_datacenter'

      expect(connect.datacenter).to eq('current_datacenter')
    end

    it "default is nil" do
      connect = Cequel.connect host: Cequel::SpecSupport::Helpers.host,
                           port: Cequel::SpecSupport::Helpers.port
      expect(connect.datacenter).to be_nil
    end
  end

  describe "#load_balancing_policy" do
    let(:datacenter) { nil }
    let(:options) do
      {
        host: Cequel::SpecSupport::Helpers.host,
        port: Cequel::SpecSupport::Helpers.port,
        datacenter: datacenter
      }
    end

    subject(:load_balancing_policy) do
      Cequel.connect(options).load_balancing_policy
    end

    context "with datacenter set" do
      let(:datacenter) { 'current_datacenter' }
      let(:token_aware_policy) { double(:token_aware_policy).as_null_object }
      let(:dc_aware_round_robin_policy) { double(:dc_aware_round_robin_policy) }

      it { is_expected.to be_a Hash }
      it { is_expected.to include(:load_balancing_policy) }

      describe "#[:load_balancing_policy]" do
        subject(:policy) { load_balancing_policy[:load_balancing_policy] }

        before do
          expect(Cassandra::LoadBalancing::Policies::DCAwareRoundRobin).to receive(:new)
                                                                       .with(datacenter)
                                                                       .and_return(dc_aware_round_robin_policy)
          expect(Cassandra::LoadBalancing::Policies::TokenAware).to receive(:new)
                                                                .with(dc_aware_round_robin_policy)
                                                                .and_return(token_aware_policy)
        end

        it { is_expected.to be token_aware_policy }
      end

      describe "client instantiation" do
        let(:connect) { Cequel.connect(options) }
        let(:policy) { connect.load_balancing_policy[:load_balancing_policy] }
        subject(:client) { connect.client }

        it "passes load_balancing_policy to Cassandra.cluster" do
          expect(Cassandra).to receive(:cluster).and_wrap_original do |m, *options|
            options = options.first
            expect(options).to include(:load_balancing_policy)
            expect(options[:load_balancing_policy]).to be policy
            m.call(options)
          end
          subject
        end
      end
    end

    context "without a datacenter" do
      it { is_expected.to be_nil }

      describe "client instantiation" do
        subject(:client) { Cequel.connect(options).client }

        it "does not pass load_balancing_policy to Cassandra.cluster" do
          expect(Cassandra).to receive(:cluster).and_wrap_original do |m, *options|
            options = options.first
            expect(options).to_not include(:load_balancing_policy)
            m.call(options)
          end
          subject
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
