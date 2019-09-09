NimbleCSV.define(HillsParser, separator: ",", escape: "\"")
NimbleCSV.define(ChaseParser, separator: ",")

defmodule BudgetReconciler do
  use Timex

  alias BudgetReconciler.{YNAB, Hills, Chase}

  @moduledoc """
  Documentation for BudgetReconciler.
  """

  @doc """
  Hello world.

  ## Examples

      iex> BudgetReconciler.hello()
      :world

  """
  def hello do
    :world
  end

  def ynab_data(which \\ "hills") do
    {:ok, data} = File.read("ynab_#{which}_data.json")
    ynab = Jason.decode!(data)
    transactions = get_in(ynab, ["data", "transactions"])

    Enum.map(transactions, fn d ->
      %{
        amount: d["amount"],
        account_name: d["account_name"],
        approved: d["approved"],
        cleared: d["cleared"],
        date: Timex.parse!(d["date"], "{YYYY}-{0M}-{0D}") |> Timex.to_date(),
        deleted: d["deleted"],
        import_id: d["import_id"],
        memo: d["memo"],
        payee_name: d["payee_name"]
      }
    end)
  end

  def hills_data() do
    {:ok, data} = File.read("hills_data.csv")

    data
    |> HillsParser.parse_string()
    |> Enum.map(fn [account_number, post_date, check, description, debit, credit, status, balance] ->
      dt = Timex.parse!(post_date, "{M}/{D}/{YYYY}") |> Timex.to_date()

      amount =
        if credit == "" do
          parse_hills_amount(debit) * 1000 * -1
        else
          parse_hills_amount(credit) * 1000
        end

      %{
        account_number: account_number,
        post_date: dt,
        check: check,
        description: description,
        debit: debit,
        amount: Kernel.trunc(amount),
        credit: credit,
        status: status,
        balance: balance
      }
    end)
  end

  def chase_data() do
    {:ok, data} = File.read("chase_data.csv")

    data
    |> ChaseParser.parse_string()
    |> Enum.map(fn [transaction_date, post_date, description, category, type, amount] ->
      %{
        transaction_date: Timex.parse!(transaction_date, "{0M}/{0D}/{YYYY}") |> Timex.to_date(),
        post_date: Timex.parse!(post_date, "{0M}/{0D}/{YYYY}") |> Timex.to_date(),
        description: description,
        category: category,
        type: type,
        amount: parse_chase_amount(amount)
      }
    end)
  end

  defp parse_chase_amount(string) do
    case Float.parse(string) do
      {num, ""} ->
        (num * 1000) |> Kernel.trunc()

      _ ->
        IO.puts("Cant parse '#{string}'")
        0
    end
  end

  defp parse_hills_amount("." <> other), do: parse_hills_amount("0." <> other)

  defp parse_hills_amount(string) when is_binary(string) do
    case Float.parse(string) do
      {num, ""} ->
        num

      _ ->
        IO.puts("Cant parse '#{string}'")
        0
    end
  end

  def load() do
    {
      ynab_data(),
      hills_data()
    }
  end

  def ynab_transactions_not_in_hills_bank({ynab, hills}) do
    unreconciled = Enum.filter(ynab, YNAB.not_reconciled?())
    min_date = Enum.min_by(unreconciled, fn x -> x[:date] end) |> Map.get(:date)
    max_date = Enum.max_by(unreconciled, fn x -> x[:date] end) |> Map.get(:date)

    hills_in_date = Enum.filter(hills, Hills.between(min_date, max_date))

    {_, not_in_hills} = Enum.split_with(unreconciled, YNAB.in_hills(hills))
    {_, not_in_ynab} = Enum.split_with(hills_in_date, Hills.in_ynab(ynab))

    IO.puts("############################################")
    IO.puts("       NOT IN HILLS BANK                    ")
    IO.puts("############################################")
    IO.inspect(not_in_hills)
    IO.puts("")
    IO.puts("")
    IO.puts("############################################")
    IO.puts("               NOT IN YNAB                  ")
    IO.puts("############################################")
    IO.puts("")
    IO.inspect(not_in_ynab)

    :ok
  end

  def is_same?(ynab_tx, hills_tx) do
    if ynab_tx.amount == hills_tx.amount do
      days_diff = Timex.diff(ynab_tx.date, hills_tx.post_date, :days) |> abs()

      days_diff <= 2
    else
      false
    end
  end

  defmodule Hills do
    def with_amount(amount) do
      fn tx ->
        tx[:amount] == amount
      end
    end

    def in_ynab(ynab) do
      fn hills_tx ->
        Enum.any?(ynab, &BudgetReconciler.is_same?(&1, hills_tx))
      end
    end

    def between(start_date \\ ~D[2019-03-01], end_date \\ ~D[2019-09-01]) do
      fn hills_tx ->
        starts_after = Date.compare(hills_tx[:post_date], start_date) in [:gt, :eq]
        starts_before = Date.compare(hills_tx[:post_date], end_date) in [:lt, :eq]

        starts_after && starts_before
      end
    end
  end

  defmodule Chase do
    def with_amount(amount) do
      fn tx ->
        tx[:amount] == amount
      end
    end

    def in_ynab(ynab) do
      fn hills_tx ->
        Enum.any?(ynab, &BudgetReconciler.is_same?(&1, hills_tx))
      end
    end

    def between(start_date \\ ~D[2019-03-01], end_date \\ ~D[2019-09-01]) do
      fn hills_tx ->
        starts_after = Date.compare(hills_tx[:post_date], start_date) in [:gt, :eq]
        starts_before = Date.compare(hills_tx[:post_date], end_date) in [:lt, :eq]

        starts_after && starts_before
      end
    end
  end

  defmodule YNAB do
    def reconciled?() do
      fn ynab_tx ->
        ynab_tx[:cleared] == "reconciled"
      end
    end

    def not_reconciled?() do
      fn ynab_tx ->
        ynab_tx[:cleared] != "reconciled"
      end
    end

    def in_hills(hills) do
      fn ynab_tx ->
        Enum.any?(hills, &BudgetReconciler.is_same?(ynab_tx, &1))
      end
    end

    def in_chase(chase) do
      fn ynab_tx ->
        Enum.any?(chase, &BudgetReconciler.is_same?(ynab_tx, &1))
      end
    end
  end
end
