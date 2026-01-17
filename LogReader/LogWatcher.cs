using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.IO;
using System.Timers;

namespace LogReader
{
  /// <summary>
  /// Наблюдатель за изменениями в лог файле.
  /// </summary>
  public class LogWatcher : IDisposable
  {
    public bool IsWatching { get; private set; }

    public delegate void BlockNewLinesHandler(List<string> lines, double progress);
    public event BlockNewLinesHandler BlockNewLines;

    public delegate void FileReCreatedHandler();
    public event FileReCreatedHandler FileReCreated;

    private readonly object readLock = new object();
    private long fileLength;
    private long position;
    private readonly string filePath;
    private Timer timer;

    private SftpClient _client;

    /// <summary>
    /// Кол-во max строк для блока записи.
    /// </summary>
    private const int LineBlockSize = 500;

    /// <summary>
    /// ctor с созданием наблюдателя за файлом по указанному пути. Триггер на изменение файла.
    /// </summary>
    /// <param name="filePath">Путь до файла.</param>
    public LogWatcher(string filePath)
    {
      this.filePath = filePath;
      fileLength = 0;
    }

    /// <summary>
    /// ctor с созданием с созданием Sftp клиента для доступа к файлам по ssh.
    /// </summary>
    /// <param name="filePath">Путь до файла.</param>
    public LogWatcher(string filePath, SftpClient client) : this(filePath)
    {
      _client = client;
    }

    public string GetLogFilePath()
    {
      return filePath;
    }

    /// <summary>
    /// При возможном сбое повторного просмотра файла - вылетает ошибка.
    /// </summary>
    /// <exception cref="Exception">Log file already watching</exception>
    public void StartWatch(int period)
    {
      if (IsWatching)
        throw new Exception("Log file already watching");

      IsWatching = true;

      timer = new Timer
      {
        AutoReset = false,
        Interval = period
      };
      timer.Elapsed += OnTimedEvent;
      timer.Start();
    }

    /// <summary>
    /// Чтение файла. При повторном срабатывании читает файл с прошлого места окончания чтения.
    /// </summary>
    public void ReadToEndLine()
    {
      lock (readLock)
      {
        using var fileStream = new FileStream(this.filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var streamReader = new StreamReader(fileStream);
        long current_length = streamReader.BaseStream.Length;
        if (current_length < fileLength)
        {
          streamReader.DiscardBufferedData();
          streamReader.BaseStream.Seek(0, SeekOrigin.Begin);
          this.position = 0;
          FileReCreated?.Invoke();
        }

        string line;
        List<string> lines = new List<string>();
        streamReader.BaseStream.Position = this.position;
        fileLength = current_length;
        while (streamReader != null && (line = streamReader.ReadLine()) != null)
        {
          if (!String.IsNullOrEmpty(line))
          {
            lines.Add(line);

            if (lines.Count >= LineBlockSize)
            {
              InvokeBlockNewLinesEvent(lines, streamReader);
              lines.Clear();
            }
          }
        }

        if (lines.Count > 0)
          InvokeBlockNewLinesEvent(lines, streamReader);
        this.position = streamReader.BaseStream.Position;

      }
    }

    /// <summary>
    /// Чтение файла на удаленной машине. При повторном срабатывании читает файл с прошлого места окончания чтения.
    /// </summary>
    public void ReadToEndLineRemote()
    {
      if (_client == null)
        return;

      _client.Connect();
      using var fileStream = _client.OpenRead(filePath);
      using var streamReader = new StreamReader(fileStream);
      var current_length = streamReader.BaseStream.Length;
      if (current_length < fileLength)
      {
        streamReader.DiscardBufferedData();
        streamReader.BaseStream.Seek(0, SeekOrigin.Begin);
        position = 0;
        FileReCreated?.Invoke();
      }

      string line;
      var lines = new List<string>();
      streamReader.BaseStream.Position = position;
      fileLength = current_length;
      while (streamReader != null && (line = streamReader.ReadLine()) != null)
      {
        if (!string.IsNullOrEmpty(line))
        {
          lines.Add(line);

          if (lines.Count >= LineBlockSize)
          {
            InvokeBlockNewLinesEvent(lines, streamReader);
            lines.Clear();
          }
        }
      }

      if (lines.Count > 0)
        InvokeBlockNewLinesEvent(lines, streamReader);

      position = streamReader.BaseStream.Position;
      _client.Disconnect();
    }

    private void OnTimedEvent(Object source, ElapsedEventArgs e)
    {
      if (_client == null)
        ReadToEndLine();
      else
        ReadToEndLineRemote();

      timer.Start();
    }

    /// <summary>
    /// Заполнение прогресса.
    /// Вызов делегата BlockNewLines.
    /// </summary>
    /// <param name="lines">Новые прочитанные строки.</param>
    /// <param name="streamReader">Экземпляр reader для работы в обработчике.</param>
    private void InvokeBlockNewLinesEvent(List<string> lines, StreamReader streamReader)
    {
      var progress = fileLength == 0 ? 100 : 100 * streamReader.BaseStream.Position / fileLength;
      BlockNewLines?.Invoke(lines, progress);
    }

    /// <summary>
    /// Обработчик закрытия файлов.
    /// </summary>
    public void Dispose()
    {
      if (timer != null)
      {
        timer.Dispose();
        timer = null;
      }
      if (_client != null)
      {
        _client.Dispose();
        _client = null;
      }
    }
  }
}
