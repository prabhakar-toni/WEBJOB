
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System.Diagnostics.CodeAnalysis;

namespace WEBJOB
{
	public class Program
	{
		public static async Task Main(string[] args)
		{
			try
			{
				#region Kafka



				//var host = CreateHostBuilder(args).Build();
				//var bidCosumer = host.Services.GetKeyedService<IKafkaConsumer>("BidConsumer");
				//var tradeCosumer = host.Services.GetKeyedService<IKafkaConsumer>("TradeConsumer");

				//var bidTask = Task.Run(() => bidCosumer.ConsumeAsync());
				//var tradeTask = Task.Run(() => tradeCosumer.ConsumeAsync());

				//await Task.WhenAll(bidTask, tradeTask);
				//Thread.Sleep(8000);
				#endregion

				#region Compare
				var bidMessage = new BidMessage
				{
					BidLoanAmount = 1500.75m,
					BidSellPricePercent = 101.25,
					NoteInterestRate = 3.85,
					MaturityTerm = 360,
					BidAcceptanceDate = new DateTime(2024, 12, 15, 14, 0, 0)
				};

				var tradeMessage = new TradeMessage
				{
					LnBal = 150000.75m,
					FnlPrc = 101.25,
					NtRt = 3.85,
					Term = 360,
					TrdDt = new DateTime(2024, 12, 15, 14, 0, 0)
				};

				var comparer = new MessageComparer();
				var status = comparer.CompareMessages(bidMessage, tradeMessage);

				if (status == MessageComparer.ComparisonStatus.Match)
				{
					Console.WriteLine("Messages match!");
				}
				else
				{
					Console.WriteLine("Messages do not match.");
				}
				#endregion
				Console.ReadKey();
			}
			catch (Exception ex)
			{

				Console.WriteLine(JsonConvert.SerializeObject(ex));
			}
		}

		static IHostBuilder CreateHostBuilder(string[] args)
		{
		return	Host.CreateDefaultBuilder(args)
				.ConfigureAppConfiguration((context, config) =>
				{
					config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
				}).ConfigureServices((context, services) =>
				{
					// Configure Kafka settings
					services.Configure<KafkaSettings>(context.Configuration.GetSection("KafkaSettings"));

					// Add database context
					services.AddDbContext<OscarDB_Context>(options =>
					{
						options.UseSqlServer(context.Configuration.GetConnectionString("OscarConnection"));
					});

					// Add scoped services
					services.AddScoped<IDataRepository, DataRepository>();
					services.AddScoped<IOscarDB_Context, OscarDB_Context>();
					services.AddTransient<IKafkaProducer, KafkaProducer>();
					services.AddKeyedSingleton<IKafkaConsumer, BidConsumer>("BidConsumer");
					services.AddKeyedSingleton<IKafkaConsumer, TradeConsumer>("TradeConsumer");

					// Add BidConsumer
					services.AddScoped<BidConsumer>(provider =>
					{
						var kafkaSettings = provider.GetRequiredService<IOptions<KafkaSettings>>().Value;
						var dataRepository = provider.GetRequiredService<IDataRepository>();

						return new BidConsumer(dataRepository, Options.Create(kafkaSettings));
					});

					// Add TradeConsumer
					services.AddScoped<TradeConsumer>(provider =>
					{
						var kafkaSettings = provider.GetRequiredService<IOptions<KafkaSettings>>().Value;
						var dataRepository = provider.GetRequiredService<IDataRepository>();
						var producer = provider.GetRequiredService<IKafkaProducer>();

						return new TradeConsumer(dataRepository, Options.Create(kafkaSettings));
					});

					
				});

		}
	}
	public class KafkaSettings
	{
		public string BootstrapServer { get; set; }
		public string BidTopic { get; set; }
		public string BidGroupid { get; set; }
		public string TradeGroupid { get; set; }
		public string TradeTopic { get; set; }
	}


	#region Models

	public class TradeMessage
	{
		public string LnNum { get; set; }
		public decimal? LnBal { get; set; }
		public double? FnlPrc { get; set; }
		public string CmmtNum { get; set; }
		public DateTime? ExpirDt { get; set; }
		public DateTime? TcRcptStamp { get; set; }
		public DateTime? TrdDt { get; set; }
		public DateTime? CltrlDlvryDt { get; set; }
		public string InvLnNum { get; set; }
		public double NtRt { get; set; }
		public int? Term { get; set; }

		public string CtrPartyCd { get; set; }

	}
	public class BidMessage
	{
		public string LoanNumber { get; set; }
		public decimal BidLoanAmount { get; set; }
		public string BidInvestorLoanNumber { get; set; }
		public double BidNoteRate { get; set; }
		public string BidCommitmentNumber { get; set; }
		public double BidSellPricePercent { get; set; }
		public string BidProductCode { get; set; }
		public DateTime ExpirationDate { get; set; }
		public DateTime BidAcceptanceDate { get; set; }
		public DateTime BidCollateralDeliveryDueDate { get; set; }
		public string CounterpartyCode { get; set; }
		public string CounterpartyOrganizationName { get; set; }
		public double BidRiskAdjustmentPricePercent { get; set; }
		public string BidTraderIdentificationCode { get; set; }
		public double BidBasePricePercent { get; set; }
		public double BidServicingRightPremiumPricePercent { get; set; }
		public double BidNetPricePercent { get; set; }
		public double BidSpecPricePercent { get; set; }
		public decimal OriginalLoanAmount { get; set; }
		public double NoteInterestRate { get; set; }
		public string LnType { get; set; }
		public string LnPurpose { get; set; }
		public int FicoNum { get; set; }
		public int OrigLtVNum { get; set; }
		public int MaturityTerm { get; set; }
	}
	public class LoanDetail
	{
		public int ID { get; set; }
		public int CurrentStatusTypeID { get; set; }
		public DateTime CurrentStatusOn { get; set; }
		public string CurrentStatusBy { get; set; }
		public int PreviousStatusTypeID { get; set; }
		public DateTime PreviousStatusOn { get; set; }
		public string PreviousStatusBy { get; set; }
		public int MilestoneTypeID { get; set; }
		public string LoanNumber { get; set; }
		public string LastCmodAction { get; set; }
		public bool Active { get; set; }
		public DateTime DeletedOn { get; set; }
		public string DeletedBy { get; set; }
		public DateTime CreatedOn { get; set; }
		public string CreatedBy { get; set; }
		public DateTime ModifiedOn { get; set; }
		public string ModifiedBy { get; set; }
	}
	public class WorkflowData
	{
		public int ID { get; set; }
		public int LoanDetailID { get; set; }
		public int WorkflowTypeID { get; set; }
		public string JSONData { get; set; }
		public DateTime ReceivedOn { get; set; }
		public string KafkaKey { get; set; }
		public DateTime? DeletedOn { get; set; }
		public string DeletedBy { get; set; }
		public DateTime CreatedOn { get; set; }
		public string CreatedBy { get; set; }
		public DateTime ModifiedOn { get; set; }
		public string ModifiedBy { get; set; }
		public int JSONDataTypeId { get; set; }
	}


	#endregion

	#region Repo
	public interface IOscarDB_Context
	{
		DbSet<LoanDetail> LoanDetail { get; set; }
		DbSet<WorkflowData> WorkflowData { get; set; }
		Task<int> SaveChangesAsync(CancellationToken cancellationToken = default);
		Task AddOrUpdateEntitiesAsync<TEntity>(TEntity entity) where TEntity : class;
	}
	public class OscarDB_Context : DbContext, IOscarDB_Context
	{
		public DbSet<LoanDetail> LoanDetail { get; set; }
		public DbSet<WorkflowData> WorkflowData { get; set; }
		// Constructor
		public OscarDB_Context(DbContextOptions<OscarDB_Context> options) : base(options)
		{


		}

		// Override SaveChangesAsync
		public override async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
		{
			int result = 0;
			try
			{
				result = await base.SaveChangesAsync(cancellationToken);
			}
			catch (Exception)
			{

				throw;
			}
			return result;
		}

		// AddOrUpdateEntitiesAsync method
		public async Task AddOrUpdateEntitiesAsync<TEntity>(TEntity entity) where TEntity : class
		{
			var entry = Entry(entity);
			if (entry.State == EntityState.Detached)
			{
				await Set<TEntity>().AddAsync(entity).ConfigureAwait(false);
			}
			else if (entry.State == EntityState.Added)
			{
				await Set<TEntity>().AddAsync(entity).ConfigureAwait(false);
			}
			else if (entry.State == EntityState.Modified)
			{
				Set<TEntity>().Update(entity);
			}
		}

		protected override void OnModelCreating(ModelBuilder modelBuilder)
		{
			modelBuilder.Entity<WorkflowData>(entity => entity.ToTable("WorkflowData"));
			modelBuilder.Entity<LoanDetail>(entity => entity.ToTable("LoanDetail"));

			base.OnModelCreating(modelBuilder);

		}
		#endregion


	}
	public class DataRepository : IDataRepository
	{
		private readonly IOscarDB_Context _Context;
		public DataRepository(IOscarDB_Context oscarDB_Context)
		{
			_Context = oscarDB_Context;
		}

		public async Task AddOrUpdateAsync<T>(T Entity) where T : class
		{
			await _Context.AddOrUpdateEntitiesAsync(Entity);
			await _Context.SaveChangesAsync();
		}

		public WorkflowData GetBiddata(int loanDetailId)
		{
			return _Context.WorkflowData.Where(x => x.LoanDetailID == loanDetailId).OrderByDescending(x => x.CreatedOn).FirstOrDefault();
		}
	}

	public interface IDataRepository
	{
		Task AddOrUpdateAsync<T>(T Entity) where T : class;
		WorkflowData GetBiddata(int loanDetailId);
	}

	public class KafkaProducer : IKafkaProducer
	{
		private readonly IProducer<string, string> _producer;
		private readonly KafkaSettings _kafkaSettings;

		public KafkaProducer(IOptions<KafkaSettings> kafkaSettings, IProducer<string, string> producer = null)
		{
			_kafkaSettings = kafkaSettings.Value;
			var pemPath = $"{AppDomain.CurrentDomain.BaseDirectory}cert\\RECON.TEST.pem";
			var keyPath = $"{AppDomain.CurrentDomain.BaseDirectory}cert\\RECON.TEST.key";

			var producerConfig = new ProducerConfig
			{
				BootstrapServers = _kafkaSettings.BootstrapServer,
				SslCaLocation = null,
				SslCertificatePem = File.ReadAllText(pemPath),
				SslKeyPem = File.ReadAllText(keyPath),
				SecurityProtocol = SecurityProtocol.Ssl
			};

			_producer = producer ?? new ProducerBuilder<string, string>(producerConfig).Build();
		}

		public void Dispose()
		{
			_producer.Dispose();
		}

		public async Task ProduceAsync(string topic, string value, Headers headers)
		{
			try
			{
				var deliveryResult = await _producer.ProduceAsync(topic, new Message<string, string>
				{
					Key = Guid.NewGuid().ToString(),
					Value = value,
					Headers = headers
				});

				Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
			}
			catch (ProduceException<string, string> e)
			{
				Console.WriteLine($"Delivery failed: {e.Error.Reason}");
			}
		}
	}

	public class BidConsumer : IKafkaConsumer
	{
		private readonly IDataRepository _dataRepository;
		private readonly KafkaSettings _kafkaSettings;
		private string _topic;

		public BidConsumer(IDataRepository dataRepository, IOptions<KafkaSettings>  kafkaSettings)
		{
			_dataRepository = dataRepository;
			_kafkaSettings = kafkaSettings.Value;
		}

		public async Task ConsumeAsync(CancellationToken cancellationToken = default)

		{

			try
			{

				var config = new ConsumerConfig
				{
					GroupId = _kafkaSettings.BidGroupid,
					BootstrapServers = "localhost:9092",
					AutoOffsetReset = AutoOffsetReset.Earliest,
					EnableAutoCommit = true,
				};

				_topic = _kafkaSettings.BidTopic;

				using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
				{
					consumer.Subscribe(_topic);

					try
					{
						while (true)
						{
							var consumeResult = consumer.Consume(cancellationToken);
							Console.WriteLine(consumeResult.Message.Value);
						
						}
					}
					catch (OperationCanceledException)
					{
						// Handle cancellation
					}
					finally
					{
						consumer.Close();
					}
				}
			}
			catch (Exception ex)
			{
				// Log exception or handle error
				Console.WriteLine($"Error occurred: {ex.Message}");
			}
		}
	}

	public class TradeConsumer : IKafkaConsumer
	{
		private readonly IDataRepository _dataRepository;
		private readonly KafkaSettings _kafkaSettings;
		private string _topic;

		public TradeConsumer(			IDataRepository dataRepository,
			IOptions<KafkaSettings> kafkaSettings)
		{
			_dataRepository = dataRepository;
			_kafkaSettings = kafkaSettings.Value;
		}

		public async Task ConsumeAsync(CancellationToken cancellationToken = default)

		{

			try
			{

				var config = new ConsumerConfig
				{
					GroupId = _kafkaSettings.TradeGroupid,
					BootstrapServers = "localhost:9092",
					AutoOffsetReset = AutoOffsetReset.Earliest,
					EnableAutoCommit = true,
				};

				_topic = _kafkaSettings.TradeTopic;

				using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
				{
					consumer.Subscribe(_topic);

					try
					{
						while (true)
						{
							var consumeResult = consumer.Consume(cancellationToken);
							Console.WriteLine(consumeResult.Message.Value);							
						}
					}
					catch (OperationCanceledException)
					{
					}
					finally
					{
						consumer.Close();
					}
				}
			}
			catch (Exception ex)
			{
				// Log exception or handle error
				Console.WriteLine($"Error occurred: {ex.Message}");
			}
		}
	}
	
	public interface IKafkaConsumer
	{
		Task ConsumeAsync(CancellationToken cancellationToken = default);
	}
}
